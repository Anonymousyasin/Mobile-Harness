package com.jarves.mh.runtime

import android.content.Context
import android.util.Log
import androidx.core.content.ContextCompat
import com.jarves.mh.model.ChatMessage
import com.jarves.mh.model.ChangeItem
import com.jarves.mh.model.DiffLine
import com.jarves.mh.model.DiffLineType
import com.jarves.mh.model.ProjectKind
import com.jarves.mh.model.ProviderProfile
import com.jarves.mh.model.RiskLevel
import com.jarves.mh.model.RuntimeEvent
import com.jarves.mh.model.ToolRequest
import java.io.File
import java.io.RandomAccessFile
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import org.json.JSONArray

internal object ProviderRuntimeErrorDetector {
    fun detect(line: String): String? {
        val json = runCatching { JSONObject(line) }.getOrNull()
        val combined = buildString {
            append(line)
            json?.let {
                append(' ')
                append(it.optString("error"))
                append(' ')
                append(it.optString("message"))
                append(' ')
                append(it.optString("result"))
            }
        }.lowercase()
        return when {
            "user not found" in combined -> "User not found. Check the API key and provider account."
            "authentication_failed" in combined ||
                "authentication failed" in combined ||
                "invalid api key" in combined ||
                "http 401" in combined ||
                (json?.optString("subtype") == "api_retry" && json.optInt("error_status") in listOf(401, 403)) ->
                "The provider rejected the saved API key."
            json?.optString("type") == "error" ->
                "Pi Agent reported an error. Check the API key and provider account."
            else -> null
        }
    }
}

class PiRuntimeBridge(
    private val context: Context,
    private val secretFor: (ProviderProfile) -> String?,
) : RuntimeBridge {
    private val installer = RuntimeInstaller(context)
    private val eventBus = MutableSharedFlow<RuntimeEvent>(extraBufferCapacity = 64)
    override val events: Flow<RuntimeEvent> = eventBus
    private val pending = ConcurrentHashMap<String, PendingPermission>()
    private val toolNames = ConcurrentHashMap<String, String>()
    private val seenToolCalls = ConcurrentHashMap.newKeySet<String>()
    private val completedToolCalls = ConcurrentHashMap.newKeySet<String>()
    private val loggedEventTypes = ConcurrentHashMap.newKeySet<String>()
    private val finishedSessions = ConcurrentHashMap.newKeySet<String>()
    private val projectRoots = ConcurrentHashMap<String, String>()
    @Volatile private var activeProcess: Process? = null
    @Volatile private var activeSessionId: String? = null
    @Volatile private var userStopRequested: Boolean = false
    @Volatile private var activeProjectSlug: String? = null
    @Volatile private var taskStartedAtElapsedRealtime: Long = 0L
    @Volatile private var lastForegroundProgressAt: Long = 0L
    @Volatile private var foregroundResultPosted: Boolean = false
    private val streamedText = StringBuilder()
    private val streamedThinking = StringBuilder()
    private var lastReasoningTokens = 0
    private var lastReasoningUpdateAt = 0L
    private var lastThinkingUpdateAt = 0L
    private var currentThinkingBlockId = 0L

    override suspend fun startSession(projectId: String, projectSlug: String, projectKind: ProjectKind, prompt: String, conversationHistory: List<ChatMessage>, provider: ProviderProfile): String = withContext(Dispatchers.IO + NonCancellable) {
        val sessionId = UUID.randomUUID().toString()
        finishedSessions.remove(sessionId)
        activeSessionId = sessionId
        userStopRequested = false
        activeProjectSlug = projectSlug
        taskStartedAtElapsedRealtime = android.os.SystemClock.elapsedRealtime()
        lastForegroundProgressAt = 0L
        foregroundResultPosted = false
        toolNames.clear()
        seenToolCalls.clear()
        completedToolCalls.clear()
        loggedEventTypes.clear()
        streamedText.clear()
        lastReasoningTokens = 0
        lastReasoningUpdateAt = 0L
        lastThinkingUpdateAt = 0L
        currentThinkingBlockId = 0L
        streamedThinking.clear()
        eventBus.emit(RuntimeEvent.SessionStarted(sessionId))
        pushForegroundProgress("Starting Pi Agent…")
        val secret = secretFor(provider).orEmpty()
        if (secret.isBlank()) {
            eventBus.emit(RuntimeEvent.SessionFailed(sessionId, "No API key is saved for ${provider.kind.title}."))
            return@withContext sessionId
        }

        var formatGateway: LocalFormatGateway? = null
        runCatching {
            RuntimeTaskController.stopAction = {
                userStopRequested = true
                val running = activeProcess
                if (running != null) {
                    Thread {
                        running.destroy()
                        Thread.sleep(500)
                        if (running.isAlive) running.destroyForcibly()
                    }.start()
                }
            }
            startForegroundRuntime(projectSlug)
            // Setup and release checks happen once in the app-start loading flow.
            // Sending a prompt must never perform network update checks or put setup
            // messages into the conversation.
            val installed = installer.installedRuntime()
            installer.ensureSettingsAndHooks()
            val workspace = ensureWorkspace(projectId)
            createCheckpoint(projectId, workspace)
            val before = snapshot(workspace)
            formatGateway = if (provider.kind.protocol in setOf(
                    com.jarves.mh.model.ProviderProtocol.OPENAI_CHAT,
                    com.jarves.mh.model.ProviderProtocol.OPENAI_RESPONSES,
                )) LocalFormatGateway(provider, secret).start() else null
            val launch = RuntimeLaunchConfigBuilder.build(provider, authToken = secret, localGatewayUrl = formatGateway?.url)
            Log.d("PiBridge", "Provider: ${provider.kind}, Model: ${provider.model}, BaseUrl: ${provider.baseUrl}")
            Log.d("PiBridge", "Launch environment keys: ${launch.environment.keys}")

            // Build a context-aware prompt that includes conversation history
            val guestWorkspacePath = "/workspace/$projectSlug"
            val contextPrompt = buildContextPrompt(prompt, conversationHistory, guestWorkspacePath, projectKind)

            val command = buildList {
                add(launch.executable)
                addAll(launch.arguments)
                add("--")
                add(contextPrompt)
            }
            // Never log argv verbatim (it may carry secrets); redact any
            // value following --api-key and drop the trailing prompt.
            val safeCommand = command.dropLast(1).mapIndexed { index, arg ->
                if (index > 0 && command[index - 1] == "--api-key") "••••" else arg
            }
            Log.d("PiBridge", "Launching command: $safeCommand + <prompt>")
            val process = installer.process(
                installed.proot,
                installed.rootfs,
                workspace,
                launch.environment,
                command,
                guestWorkspacePath = guestWorkspacePath,
            )
            activeProcess = process
            // Pi merges piped stdin into the prompt in print mode, but this
            // session never writes to stdin: close it so Pi sees EOF instead
            // of blocking forever on an open pipe. Terminal sessions use
            // separate process instances and keep their stdin.
            runCatching { process.outputStream.close() }
            if (userStopRequested) process.destroy()
            coroutineScope {
                val permissionWatcher = launch { watchPermissionRequests(sessionId) }
                var lastDiagnostic = ""
                // Keep the TAIL of all unconsumed output (multi-line, capped):
                // a Bun/runtime crash often prints the cause several lines up.
                fun appendDiagnostic(line: String) {
                    if (line.isBlank()) return
                    lastDiagnostic = (if (lastDiagnostic.isEmpty()) line else "$lastDiagnostic\n$line").takeLast(2000)
                }
                val pendingOutput = StringBuilder()
                val nativeProcess = process as? NativeSpawnProcess
                    ?: error("Unsupported Android runtime process")
                var outputOffset = 0L
                while (process.isAlive || nativeProcess.outputFile.length() > outputOffset) {
                    val available = nativeProcess.outputFile.length() - outputOffset
                    if (available <= 0) {
                        delay(50)
                        continue
                    }
                    val bytes = ByteArray(minOf(available, 16L * 1024).toInt())
                    val count = RandomAccessFile(nativeProcess.outputFile, "r").use { file ->
                        file.seek(outputOffset)
                        file.read(bytes)
                    }
                    if (count > 0) {
                        outputOffset += count
                        pendingOutput.append(bytes.decodeToString(0, count))
                        var newline = pendingOutput.indexOf("\n")
                        while (newline >= 0) {
                            val line = pendingOutput.substring(0, newline).trimEnd('\r')
                            pendingOutput.delete(0, newline + 1)
                            if (line.isNotBlank()) {
                                Log.d("PiBridge", "OUTPUT: $line")
                                ProviderRuntimeErrorDetector.detect(line)?.let { reason ->
                                    process.destroyForcibly()
                                    throw ProviderSessionException(reason)
                                }
                                if (!consumePiEvent(sessionId, line)) {
                                    appendDiagnostic(line)
                                    terminalStatus(line)?.let { (title, detail) ->
                                        eventBus.emit(RuntimeEvent.RuntimeLog(sessionId, title, detail))
                                    }
                                }
                            }
                            newline = pendingOutput.indexOf("\n")
                        }
                    }
                }
                pendingOutput.toString().trim().takeIf(String::isNotBlank)?.let { line ->
                    Log.d("PiBridge", "TRAILING OUTPUT: $line")
                    if (!consumePiEvent(sessionId, line)) appendDiagnostic(line)
                }
                val exit = process.waitFor()
                Log.d("PiBridge", "Process exited with code $exit")
                permissionWatcher.cancelAndJoin()
                pending.values.filter { it.request.sessionId == sessionId }.forEach { permission ->
                    permission.response.writeText("deny")
                    pending.remove(permission.request.approvalId)
                }
                val changed = changedFiles(workspace, before)
                if (changed.isNotEmpty()) {
                    Log.d("PiBridge", "Changed files: $changed")
                    saveChangedPaths(projectId, changed)
                    val details = loadPendingChanges(projectId)
                    eventBus.emit(RuntimeEvent.FilesChanged(sessionId, details))
                } else if (!File(checkpointDir(projectId), "changes.json").isFile) {
                    acceptLastChanges(projectId)
                }
                if (exit == 0) {
                    emitCompletedOnce(sessionId)
                    finishForegroundRuntime(
                        completed = true,
                        projectName = projectSlug,
                        detail = "Pi Agent finished the task in $projectSlug.",
                    )
                } else {
                    if (userStopRequested) throw ProviderSessionException("Stopped by user")
                    // ProviderSessionException bypasses friendlyError truncation
                    // so the full crash tail stays visible in the activity feed.
                    throw ProviderSessionException(
                        "Pi Agent exited (code $exit): ${lastDiagnostic.ifBlank { "no output" }}",
                    )
                }
            }
        }.onFailure { error ->
            Log.e("PiBridge", "Session failed", error)
            val message = friendlyError(error)
            emitFailureOnce(sessionId, message)
            if (userStopRequested) {
                cancelForegroundRuntime()
            } else {
                finishForegroundRuntime(
                    completed = false,
                    projectName = projectSlug,
                    detail = message,
                )
            }
        }
        formatGateway?.close()
        activeProcess = null
        activeSessionId = null
        RuntimeTaskController.stopAction = null
        sessionId
    }

    override suspend fun respondToApproval(request: ToolRequest, approved: Boolean) = withContext(Dispatchers.IO) {
        val permission = pending.remove(request.approvalId) ?: return@withContext
        permission.response.writeText(if (approved) "allow" else "deny")
        eventBus.emit(
            if (approved) RuntimeEvent.ToolApproved(request.sessionId, request.approvalId)
            else RuntimeEvent.ToolRejected(request.sessionId, request.approvalId),
        )
    }

    override suspend fun stopSession(sessionId: String) = withContext(Dispatchers.IO) {
        if (activeSessionId == sessionId) {
            userStopRequested = true
            activeProcess?.destroy()
            delay(500)
            if (activeProcess?.isAlive == true) activeProcess?.destroyForcibly()
            emitFailureOnce(sessionId, "Stopped by user")
        }
    }

    override suspend fun stopActiveSession() {
        activeSessionId?.let { stopSession(it) }
    }

    override suspend fun undoLastChanges(projectId: String): Boolean = withContext(Dispatchers.IO) {
        val checkpoint = checkpointDir(projectId)
        val backup = File(checkpoint, "project")
        val manifest = File(checkpoint, "changes.json")
        if (!backup.isDirectory || !manifest.isFile) return@withContext false
        val workspace = ensureWorkspace(projectId)
        val paths = runCatching {
            val array = JSONArray(manifest.readText())
            (0 until array.length()).map(array::getString)
        }.getOrElse { return@withContext false }.filterNot(::isInternalRuntimePath)

        paths.forEach { path ->
            val target = safeWorkspaceFile(workspace, path)
            val original = safeWorkspaceFile(backup, path)
            if (original.isFile) {
                target.parentFile?.mkdirs()
                original.copyTo(target, overwrite = true)
            } else if (target.isFile) {
                target.delete()
            }
        }
        checkpoint.deleteRecursively()
        true
    }

    override suspend fun acceptLastChanges(projectId: String) {
        withContext(Dispatchers.IO) {
            checkpointDir(projectId).deleteRecursively()
        }
    }

    override suspend fun loadPendingChanges(projectId: String): List<ChangeItem> = withContext(Dispatchers.IO) {
        val workspace = ensureWorkspace(projectId)
        val paths = readChangedPaths(projectId).filterNot(::isInternalRuntimePath)
        if (paths.isEmpty()) emptyList() else buildChangeDetails(projectId, workspace, paths)
    }

    override suspend fun undoFileChange(projectId: String, path: String): Boolean = withContext(Dispatchers.IO) {
        if (isInternalRuntimePath(path) || path !in readChangedPaths(projectId)) return@withContext false
        val workspace = ensureWorkspace(projectId)
        val backup = File(checkpointDir(projectId), "project")
        val target = safeWorkspaceFile(workspace, path)
        val original = safeWorkspaceFile(backup, path)
        if (original.isFile) {
            target.parentFile?.mkdirs()
            original.copyTo(target, overwrite = true)
        } else if (target.isFile) {
            target.delete()
        }
        removeChangedPath(projectId, path)
        true
    }

    override suspend fun acceptFileChange(projectId: String, path: String): Boolean = withContext(Dispatchers.IO) {
        if (isInternalRuntimePath(path) || path !in readChangedPaths(projectId)) return@withContext false
        val workspace = ensureWorkspace(projectId)
        val backup = File(checkpointDir(projectId), "project")
        val current = safeWorkspaceFile(workspace, path)
        val baseline = safeWorkspaceFile(backup, path)
        if (current.isFile) {
            baseline.parentFile?.mkdirs()
            current.copyTo(baseline, overwrite = true)
        } else if (baseline.isFile) {
            baseline.delete()
        }
        removeChangedPath(projectId, path)
        true
    }

    /**
     * Pi runs trusted with no permission hook, so this watcher is a no-op
     * safety net: anything dropped into runtime-bridge is auto-allowed so a
     * session can never stall waiting on an approval that has no UI.
     */
    private suspend fun watchPermissionRequests(sessionId: String) {
        val bridge = File(context.filesDir, "runtime-bridge")
        while (kotlin.coroutines.coroutineContext.isActive) {
            bridge.listFiles { file -> file.name.endsWith(".request") }.orEmpty().forEach { file ->
                val approvalId = file.name.removeSuffix(".request")
                runCatching {
                    val json = JSONObject(file.readText())
                    val toolName = json.optString("tool_name", "Tool")
                    val input = json.optJSONObject("tool_input") ?: JSONObject()
                    val command = input.optString("command").ifBlank { null }
                    val paths = listOf("file_path", "path", "notebook_path")
                        .mapNotNull { key -> input.optString(key).takeIf(String::isNotBlank) }
                    val explanation = input.optString("description")
                        .ifBlank { command.orEmpty() }
                        .ifBlank { "$toolName running in project" }

                    Log.d("PiBridge", "Auto-approving permission request $approvalId for $toolName ($paths)")
                    val response = File(file.parentFile, "$approvalId.response")
                    response.writeText("allow")

                    eventBus.emit(RuntimeEvent.ToolCompleted(sessionId, toolName, explanation))
                }.onFailure {
                    File(file.parentFile, "$approvalId.response").writeText("allow")
                }
            }
            delay(50)
        }
    }

    private suspend fun consumePiEvent(sessionId: String, line: String): Boolean {
        val json = runCatching { JSONObject(line) }.getOrNull() ?: return false
        consumePiJsonEvent(sessionId, json)
        return true
    }

    /**
     * Consumes Pi's `--mode json` event stream (see pi docs/json.md).
     * Terminal event is `agent_end`; per-message errors arrive as
     * `message_end` with stopReason error/aborted or an `error` update.
     */
    private suspend fun consumePiJsonEvent(sessionId: String, json: JSONObject) {
        when (json.optString("type")) {
            "session", "agent_start", "turn_start", "message_start",
            "queue_update", "compaction_start", "compaction_end",
            -> Unit
            "message_update" -> {
                val update = json.optJSONObject("assistantMessageEvent") ?: return
                when (update.optString("type")) {
                    "text_delta" -> update.optString("delta").takeIf(String::isNotEmpty)?.let {
                        streamedText.append(it)
                        eventBus.emit(RuntimeEvent.AssistantDelta(sessionId, it))
                    }
                    "thinking_delta" -> update.optString("delta").takeIf(String::isNotEmpty)?.let {
                        streamedThinking.append(it)
                        emitReasoningSummary(sessionId, streamedThinking.toString())
                    }
                    "thinking_end" -> update.optString("content").takeIf(String::isNotBlank)?.let {
                        streamedThinking.clear()
                        streamedThinking.append(it)
                        emitReasoningSummary(sessionId, it, force = true, isFinal = true)
                    }
                    "toolcall_start" -> {
                        val id = update.optString("id")
                        val name = update.optString("toolName", "Tool")
                        if (id.isNotBlank()) {
                            if (!seenToolCalls.add(id)) return
                            toolNames[id] = name
                        }
                        val args = update.optJSONObject("args")
                            ?: update.optJSONObject("arguments")
                            ?: update.optJSONObject("toolCall")?.optJSONObject("arguments")
                        emitPiToolStarted(sessionId, id, name, args ?: JSONObject())
                    }
                    "toolcall_end" -> {
                        val toolCall = update.optJSONObject("toolCall") ?: return
                        val id = toolCall.optString("id")
                        val name = toolCall.optString("name", "Tool")
                        if (id.isNotBlank()) {
                            if (!seenToolCalls.add(id)) return
                            toolNames[id] = name
                        }
                        emitPiToolStarted(sessionId, id, name, toolCall.optJSONObject("arguments") ?: JSONObject())
                    }
                    "error" -> {
                        val message = update.optJSONObject("error")?.optString("errorMessage")
                            ?: update.optString("error").ifBlank { "Pi Agent reported an error" }
                        throw IllegalStateException(message)
                    }
                }
            }
            "message_end" -> {
                val message = json.optJSONObject("message") ?: return
                if (message.optString("stopReason") in listOf("error", "aborted")) {
                    throw IllegalStateException(
                        message.optString("errorMessage").ifBlank { "Pi Agent reported an error" },
                    )
                }
                // Fallback: if no streaming deltas arrived (e.g. a newer Pi
                // changed the delta shape), still render the final text so a
                // completed turn never looks like "no response".
                if (streamedText.isEmpty()) {
                    val content = message.optJSONArray("content") ?: return
                    for (index in 0 until content.length()) {
                        val block = content.optJSONObject(index) ?: continue
                        if (block.optString("type") == "text") {
                            block.optString("text").takeIf(String::isNotBlank)?.let {
                                streamedText.append(it)
                                eventBus.emit(RuntimeEvent.AssistantDelta(sessionId, it))
                            }
                        }
                    }
                }
            }
            "tool_execution_start" -> {
                val id = json.optString("toolCallId")
                val name = json.optString("toolName", "Tool")
                if (id.isNotBlank()) {
                    if (!seenToolCalls.add(id)) return
                    toolNames[id] = name
                }
                emitPiToolStarted(sessionId, id, name, json.optJSONObject("args") ?: JSONObject())
            }
            "tool_execution_end" -> {
                val id = json.optString("toolCallId")
                val name = json.optString("toolName").ifBlank { toolNames.remove(id) ?: "Tool" }
                toolNames.remove(id)
                completedToolCalls.add(id)
                val result = json.opt("result")?.let { piResultText(it) } ?: "Completed successfully"
                // A failed tool call is normal agent life, not a session failure:
                // report it and let Pi continue. Fatal errors arrive as
                // message_end/error events or a non-zero exit code.
                val summary = if (json.optBoolean("isError")) "Tool failed: $result" else result
                eventBus.emit(RuntimeEvent.ToolCompleted(sessionId, name, sanitizeForDisplay(summary)))
            }
            "turn_end" -> {
                // Complete any tool results not already closed by tool_execution_end.
                val results = json.optJSONArray("toolResults") ?: return
                for (index in 0 until results.length()) {
                    val block = results.optJSONObject(index) ?: continue
                    val id = block.optString("toolCallId")
                    if (id.isNotBlank() && !completedToolCalls.add(id)) continue
                    val name = block.optString("toolName").ifBlank { toolNames.remove(id) ?: "Tool" }
                    toolNames.remove(id)
                    eventBus.emit(
                        RuntimeEvent.ToolCompleted(sessionId, name, sanitizeForDisplay(piResultText(block))),
                    )
                }
            }
            "agent_end" -> {
                // The structured agent_end is Pi's authoritative terminal event.
                // Update the UI immediately instead of waiting for a PRoot/Node wrapper
                // that may remain alive after the answer has already completed.
                emitCompletedOnce(sessionId)
                terminateActiveProcessGracefully()
            }
            // Never silently drop unknown events: surface a new Pi schema in
            // the activity feed (once per type) so drift is visible, not silent.
            else -> {
                val type = json.optString("type").ifBlank { "<untitled>" }
                if (loggedEventTypes.add(type)) {
                    eventBus.emit(RuntimeEvent.RuntimeLog(sessionId, "Agent event", type))
                }
            }
        }
    }

    private fun piResultText(value: Any?): String {
        if (value == null || value == JSONObject.NULL) return "Completed successfully"
        if (value is JSONObject) {
            if (value.optBoolean("is_error") || value.optBoolean("isError")) {
                return value.optString("content").ifBlank { "Tool failed" }
            }
            val content = value.optJSONArray("content")
            if (content != null) {
                val texts = (0 until content.length()).mapNotNull { content.optJSONObject(it)?.optString("text") }
                    .filter(String::isNotBlank)
                if (texts.isNotEmpty()) return texts.joinToString("\n")
            }
            return value.optString("content")
                .ifBlank { value.optString("text") }
                .ifBlank { "Completed successfully" }
        }
        return value.toString().ifBlank { "Completed successfully" }
    }

    private suspend fun emitReasoningSummary(
        sessionId: String,
        text: String,
        force: Boolean = false,
        startsNewBlock: Boolean = false,
        isFinal: Boolean = false,
    ) {
        val summary = sanitizeForDisplay(text).trim().take(2_000)
        if (summary.isBlank() && !startsNewBlock) return
        val now = android.os.SystemClock.elapsedRealtime()
        if (startsNewBlock || isFinal || force || now - lastThinkingUpdateAt >= 150) {
            lastThinkingUpdateAt = now
            eventBus.emit(
                RuntimeEvent.ReasoningSummary(
                    sessionId = sessionId,
                    summary = summary,
                    blockId = currentThinkingBlockId,
                    startsNewBlock = startsNewBlock,
                    isFinal = isFinal,
                ),
            )
            pushForegroundProgress("Thinking…")
        }
    }

    private suspend fun emitReasoningProgress(sessionId: String, tokens: Int) {
        if (tokens <= 0) return
        val now = android.os.SystemClock.elapsedRealtime()
        if (tokens - lastReasoningTokens >= 25 || now - lastReasoningUpdateAt >= 500) {
            lastReasoningTokens = tokens
            lastReasoningUpdateAt = now
            eventBus.emit(RuntimeEvent.ReasoningProgress(sessionId, tokens))
            pushForegroundProgress("Thinking…")
        }
    }

    private suspend fun emitPiToolStarted(sessionId: String, id: String, name: String, args: JSONObject) {
        val detail = when (name) {
            "bash", "Bash" -> args.optString("command").ifBlank { args.optString("description") }
            "write", "edit", "read", "Write", "Edit", "Read", "NotebookEdit" ->
                args.optString("path").ifBlank { args.optString("file_path") }.ifBlank { args.optString("notebook_path") }
            "glob", "Glob" -> args.optString("pattern")
            "grep", "Grep" -> args.optString("pattern").let { pattern ->
                args.optString("path").takeIf(String::isNotBlank)?.let { "$pattern in $it" } ?: pattern
            }
            else -> args.optString("description").ifBlank { "Running $name" }
        }
        eventBus.emit(RuntimeEvent.ToolStarted(sessionId, name, sanitizeForDisplay(detail.ifBlank { "Running $name" })))
        pushForegroundProgress("Running $name · ${detail.replace(Regex("\\s+"), " ").trim().take(80).ifBlank { name }}")
    }

    private fun terminalStatus(line: String): Pair<String, String>? = null

    private suspend fun emitCompletedOnce(sessionId: String) {
        if (finishedSessions.add(sessionId)) {
            eventBus.emit(RuntimeEvent.SessionCompleted(sessionId))
            // Post the completion notification immediately. Waiting for process
            // teardown is unsafe: the PRoot/Node wrapper can hang after the answer
            // is already done, which would freeze the notification on its last step.
            finishForegroundRuntime(
                completed = true,
                projectName = activeProjectSlug ?: "your project",
                detail = "Pi Agent finished the task.",
            )
        }
    }

    private suspend fun emitFailureOnce(sessionId: String, reason: String) {
        if (finishedSessions.add(sessionId)) {
            eventBus.emit(RuntimeEvent.SessionFailed(sessionId, reason))
            if (userStopRequested) {
                cancelForegroundRuntime()
            } else {
                finishForegroundRuntime(
                    completed = false,
                    projectName = activeProjectSlug ?: "your project",
                    detail = reason,
                )
            }
        }
    }

    private fun sanitizeForDisplay(value: String): String {
        return value
            .replace(Regex("sk-[A-Za-z0-9_-]{8,}"), "sk-••••")
            .replace(Regex("(?i)(authorization|api[_-]?key)\\s*[:=]\\s*[^\\s,}]+"), "${'$'}1: ••••")
            .replace(Regex("\\s+"), " ")
            .trim()
            .take(600)
    }

    private fun buildContextPrompt(currentPrompt: String, history: List<ChatMessage>, guestWorkspacePath: String, projectKind: ProjectKind): String {
        // Filter out the current prompt (last user message), system greeting, and any error messages
        val priorMessages = history
            .filter { msg ->
                (msg.fromUser || !msg.text.startsWith("Hi! Tell me")) &&
                !msg.text.startsWith("Failed to") &&
                !msg.text.startsWith("Error:") &&
                !msg.text.contains("API Error")
            }
            .dropLast(1) // Drop the current prompt which was just added

        val sb = StringBuilder()
        sb.appendLine("<project_workspace>")
        if (projectKind == ProjectKind.QUICK_PROJECT) {
            sb.appendLine("This is a lightweight project workspace at $guestWorkspacePath.")
            sb.appendLine("Respond conversationally, and use terminal or file tools whenever they are useful for the request.")
            sb.appendLine("Keep every file and command inside this project workspace.")
        } else {
            sb.appendLine("The current working directory $guestWorkspacePath is the project root.")
            sb.appendLine("Create and edit project files directly in this directory. Do not create another outer project folder unless the user explicitly asks for one.")
            sb.appendLine("When giving commands to the user, make them runnable from this project root.")
        }
        sb.appendLine("If this is an Android project, the phone already provides JDK 17, Android SDK 36, ARM64 Build Tools 35.0.0, Gradle 8.14.3, and an offline Maven repository.")
        sb.appendLine("For newly created Android projects, use AGP 8.11.0, Kotlin 1.9.22, compileSdk 36, and Java 17 so the preinstalled offline toolchain can build immediately.")
        sb.appendLine("The bundled Maven cache handles the base toolchain; Gradle may download project-specific libraries normally. Set android.useAndroidX=true for AndroidX or Compose projects.")
        sb.appendLine("PocketDev globally configures Gradle to use the SDK's ARM64 aapt2. Do not use the x86_64 Maven aapt2, investigate its architecture, or add android.aapt2FromMavenOverride to the project.")
        sb.appendLine("Use the installed `gradle` command for Android builds; do not ask the user to install Android Studio, an SDK, Gradle, ADB, or Termux.")
        sb.appendLine("For local servers, give a clear start command and never use a kill command that searches its own command text with pgrep, because it can terminate the terminal itself.")
        sb.appendLine("</project_workspace>")
        sb.appendLine()
        if (priorMessages.isEmpty()) {
            sb.appendLine(currentPrompt)
            return sb.toString()
        }
        sb.appendLine("<conversation_history>")
        sb.appendLine("The following is our prior conversation in this project. Continue naturally from where we left off.")
        sb.appendLine()
        for (msg in priorMessages) {
            val role = if (msg.fromUser) "User" else "Assistant"
            sb.appendLine("$role: ${msg.text}")
            if (msg.attachments.isNotEmpty()) {
                sb.appendLine("Attached files:")
                msg.attachments.forEach { attachment ->
                    sb.appendLine("- ${attachment.displayName}: $guestWorkspacePath/${attachment.relativePath} (${attachment.mimeType})")
                }
            }
            sb.appendLine()
        }
        sb.appendLine("</conversation_history>")
        sb.appendLine()
        sb.appendLine("Now, respond to this new message from the user:")
        sb.appendLine(currentPrompt)
        return sb.toString()
    }

    private fun ensureWorkspace(projectId: String): File {
        val base = File(context.filesDir, "workspaces/$projectId").apply { mkdirs() }.canonicalFile
        val rootPath = projectRoots[projectId].orEmpty()
        if (rootPath.isBlank()) return base
        val selected = File(base, rootPath).canonicalFile
        require(selected.toPath().startsWith(base.toPath())) { "Unsafe project root" }
        return selected.apply { mkdirs() }
    }

    fun configureProjectRoot(projectId: String, rootPath: String) {
        val normalized = rootPath.trim().trim('/')
        require(normalized.isBlank() || (!normalized.contains("..") && !normalized.startsWith('/'))) {
            "Unsafe project root"
        }
        val previous = projectRoots.put(projectId, normalized).orEmpty()
        if (previous != normalized) checkpointDir(projectId).deleteRecursively()
    }

    private fun checkpointDir(projectId: String) = File(context.filesDir, "checkpoints/$projectId/latest")

    private fun createCheckpoint(projectId: String, workspace: File) {
        val checkpoint = checkpointDir(projectId)
        // Keep the original baseline until every pending file is accepted or undone.
        if (File(checkpoint, "project").isDirectory && File(checkpoint, "changes.json").isFile) return
        checkpoint.deleteRecursively()
        val backup = File(checkpoint, "project").apply { mkdirs() }
        val workspacePath = workspace.canonicalFile.toPath()
        workspace.walkTopDown()
            .onEnter { directory ->
                directory == workspace || (
                    !java.nio.file.Files.isSymbolicLink(directory.toPath()) &&
                        runCatching { directory.canonicalFile.toPath().startsWith(workspacePath) }.getOrDefault(false)
                    )
            }
            .filter {
                it.isFile &&
                    !isInternalRuntimePath(it.relativeTo(workspace).invariantSeparatorsPath) &&
                    !java.nio.file.Files.isSymbolicLink(it.toPath())
            }
            .forEach { source ->
                val relative = source.relativeTo(workspace).invariantSeparatorsPath
                val destination = safeWorkspaceFile(backup, relative)
                destination.parentFile?.mkdirs()
                source.copyTo(destination, overwrite = true)
            }
    }

    private fun saveChangedPaths(projectId: String, paths: List<String>) {
        val manifest = File(checkpointDir(projectId), "changes.json")
        manifest.parentFile?.mkdirs()
        val merged = (readChangedPaths(projectId) + paths)
            .filterNot(::isInternalRuntimePath)
            .distinct()
            .sorted()
        manifest.writeText(JSONArray(merged).toString())
    }

    private fun readChangedPaths(projectId: String): List<String> {
        val manifest = File(checkpointDir(projectId), "changes.json")
        if (!manifest.isFile) return emptyList()
        return runCatching {
            val array = JSONArray(manifest.readText())
            (0 until array.length()).map(array::getString)
        }.getOrDefault(emptyList())
    }

    private fun removeChangedPath(projectId: String, path: String) {
        val remaining = readChangedPaths(projectId).filterNot { it == path }
        if (remaining.isEmpty()) {
            checkpointDir(projectId).deleteRecursively()
        } else {
            File(checkpointDir(projectId), "changes.json").writeText(JSONArray(remaining).toString())
        }
    }

    private fun buildChangeDetails(projectId: String, workspace: File, paths: List<String>): List<ChangeItem> {
        val backup = File(checkpointDir(projectId), "project")
        return paths.map { path ->
            val before = safeWorkspaceFile(backup, path).takeIf(File::isFile)?.readBytes() ?: ByteArray(0)
            val after = safeWorkspaceFile(workspace, path).takeIf(File::isFile)?.readBytes() ?: ByteArray(0)
            val binary = before.any { it == 0.toByte() } || after.any { it == 0.toByte() }
            val (additions, deletions) = lineChanges(before, after)
            ChangeItem(
                path = path,
                additions = additions,
                deletions = deletions,
                diffLines = buildDiffLines(before, after),
                binary = binary,
            )
        }
    }

    private fun buildDiffLines(beforeBytes: ByteArray, afterBytes: ByteArray): List<DiffLine> {
        if (beforeBytes.any { it == 0.toByte() } || afterBytes.any { it == 0.toByte() }) {
            return listOf(DiffLine(DiffLineType.INFO, "Binary file changed"))
        }
        val before = textLines(beforeBytes)
        val after = textLines(afterBytes)
        if (before.size > MAX_RENDERED_DIFF_LINES || after.size > MAX_RENDERED_DIFF_LINES) {
            return listOf(
                DiffLine(
                    DiffLineType.INFO,
                    "Diff is too large to display (${before.size} → ${after.size} lines). Undo and Keep still work.",
                ),
            )
        }

        val lcs = Array(before.size + 1) { IntArray(after.size + 1) }
        for (oldIndex in before.lastIndex downTo 0) {
            for (newIndex in after.lastIndex downTo 0) {
                lcs[oldIndex][newIndex] = if (before[oldIndex] == after[newIndex]) {
                    lcs[oldIndex + 1][newIndex + 1] + 1
                } else {
                    maxOf(lcs[oldIndex + 1][newIndex], lcs[oldIndex][newIndex + 1])
                }
            }
        }

        val result = mutableListOf<DiffLine>()
        var oldIndex = 0
        var newIndex = 0
        while (oldIndex < before.size || newIndex < after.size) {
            when {
                oldIndex < before.size && newIndex < after.size && before[oldIndex] == after[newIndex] -> {
                    result += DiffLine(DiffLineType.CONTEXT, before[oldIndex], oldIndex + 1, newIndex + 1)
                    oldIndex++
                    newIndex++
                }
                newIndex < after.size && (oldIndex == before.size || lcs[oldIndex][newIndex + 1] >= lcs[oldIndex + 1][newIndex]) -> {
                    result += DiffLine(DiffLineType.ADDITION, after[newIndex], null, newIndex + 1)
                    newIndex++
                }
                oldIndex < before.size -> {
                    result += DiffLine(DiffLineType.DELETION, before[oldIndex], oldIndex + 1, null)
                    oldIndex++
                }
            }
        }
        return collapseUnchangedLines(result)
    }

    private fun collapseUnchangedLines(lines: List<DiffLine>): List<DiffLine> {
        val changedIndexes = lines.indices.filter { lines[it].type != DiffLineType.CONTEXT }
        if (changedIndexes.isEmpty()) return lines
        val visible = BooleanArray(lines.size)
        changedIndexes.forEach { changed ->
            for (index in maxOf(0, changed - DIFF_CONTEXT_LINES)..minOf(lines.lastIndex, changed + DIFF_CONTEXT_LINES)) {
                visible[index] = true
            }
        }
        val result = mutableListOf<DiffLine>()
        var index = 0
        while (index < lines.size) {
            if (visible[index]) {
                result += lines[index++]
            } else {
                val start = index
                while (index < lines.size && !visible[index]) index++
                result += DiffLine(DiffLineType.INFO, "… ${index - start} unchanged lines …")
            }
        }
        return result
    }

    private fun lineChanges(beforeBytes: ByteArray, afterBytes: ByteArray): Pair<Int, Int> {
        if (beforeBytes.any { it == 0.toByte() } || afterBytes.any { it == 0.toByte() }) {
            return (if (afterBytes.isNotEmpty()) 1 else 0) to (if (beforeBytes.isNotEmpty()) 1 else 0)
        }
        val before = textLines(beforeBytes)
        val after = textLines(afterBytes)
        if (before.size > MAX_DIFF_LINES || after.size > MAX_DIFF_LINES) {
            return maxOf(0, after.size - before.size) to maxOf(0, before.size - after.size)
        }
        var previous = IntArray(after.size + 1)
        before.forEach { oldLine ->
            val current = IntArray(after.size + 1)
            after.forEachIndexed { index, newLine ->
                current[index + 1] = if (oldLine == newLine) {
                    previous[index] + 1
                } else {
                    maxOf(previous[index + 1], current[index])
                }
            }
            previous = current
        }
        val common = previous[after.size]
        return (after.size - common) to (before.size - common)
    }

    private fun textLines(bytes: ByteArray): List<String> {
        if (bytes.isEmpty()) return emptyList()
        val lines = bytes.decodeToString().split('\n')
        return if (lines.lastOrNull().isNullOrEmpty()) lines.dropLast(1) else lines
    }

    private fun safeWorkspaceFile(root: File, relative: String): File {
        require(relative.isNotBlank() && !relative.startsWith('/')) { "Unsafe workspace path" }
        val file = File(root, relative)
        val rootPath = root.canonicalFile.toPath()
        val parentPath = (file.parentFile ?: root).canonicalFile.toPath()
        require(parentPath.startsWith(rootPath)) { "Workspace path escapes project" }
        return file
    }

    private fun snapshot(root: File): Map<String, String> = root.walkTopDown()
        .filter { it.isFile && !isInternalRuntimePath(it.relativeTo(root).invariantSeparatorsPath) }
        .associate { it.relativeTo(root).path to digest(it) }

    private fun changedFiles(root: File, before: Map<String, String>): List<String> {
        val after = snapshot(root)
        return (before.keys + after.keys).distinct().filter { before[it] != after[it] }.sorted()
    }

    private fun isInternalRuntimePath(path: String): Boolean {
        val normalized = path.replace('\\', '/')
        // Pi metadata plus legacy Claude metadata from older installs.
        return normalized == ".pi" || normalized.startsWith(".pi/") ||
            normalized == ".claude" || normalized == ".claude.json" || normalized.startsWith(".claude/")
    }

    private fun digest(file: File): String {
        val digest = MessageDigest.getInstance("SHA-256")
        file.inputStream().use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private fun classifyRisk(tool: String, command: String?): RiskLevel {
        val preview = "${tool.lowercase()} ${command.orEmpty().lowercase()}"
        return when {
            listOf("rm -rf", "git push", "git reset", "sudo", "curl ").any(preview::contains) -> RiskLevel.HIGH
            tool in listOf("Write", "Edit", "NotebookEdit", "Bash") -> RiskLevel.REVIEW
            else -> RiskLevel.SAFE
        }
    }

    private fun friendlyError(error: Throwable): String {
        val message = error.message.orEmpty()
        return when {
            error is ProviderSessionException -> message
            message.contains("user not found", true) -> "User not found. Check the API key and provider account."
            message.contains("checksum", true) -> "Runtime verification failed. Nothing unverified was executed."
            message.contains("HTTP 401", true) || message.contains("authentication", true) -> "The provider rejected the saved API key."
            message.isBlank() -> "The Pi Agent runtime could not start."
            else -> message.take(2000)
        }
    }

    /**
     * Mirrors what Pi Agent is doing right now into the foreground-service
     * notification, so the notification panel shows the real task progress.
     * Throttled because each update is a service round-trip.
     */
    private fun pushForegroundProgress(detailRaw: String) {
        if (activeSessionId == null) return
        val now = android.os.SystemClock.elapsedRealtime()
        if (now - lastForegroundProgressAt < FOREGROUND_PROGRESS_MIN_INTERVAL_MS) return
        lastForegroundProgressAt = now
        val detail = detailRaw.replace(Regex("\\s+"), " ").trim().take(110)
        val elapsedMs = taskStartedAtElapsedRealtime.takeIf { it > 0 }?.let { now - it } ?: 0L
        val text = if (elapsedMs > 0L) "$detail · ${formatElapsedShort(elapsedMs)}" else detail
        runCatching {
            context.startService(
                android.content.Intent(context, RuntimeExecutionService::class.java)
                    .setAction(RuntimeExecutionService.ACTION_PROGRESS)
                    .putExtra(RuntimeExecutionService.EXTRA_PROJECT_NAME, activeProjectSlug)
                    .putExtra(RuntimeExecutionService.EXTRA_DETAIL, text),
            )
        }
    }

    private fun formatElapsedShort(milliseconds: Long): String {
        val totalSeconds = milliseconds / 1_000L
        val hours = totalSeconds / 3_600L
        val minutes = (totalSeconds % 3_600L) / 60L
        val seconds = totalSeconds % 60L
        return if (hours > 0) "%d:%02d:%02d".format(hours, minutes, seconds) else "%d:%02d".format(minutes, seconds)
    }

    /**
     * Destroys the CLI process, escalating to a force kill if the PRoot/Node
     * wrapper ignores the graceful signal. Without this, a hung wrapper would
     * block session cleanup forever after the answer was already delivered.
     */
    private fun terminateActiveProcessGracefully() {
        val running = activeProcess ?: return
        Thread {
            runCatching {
                running.destroy()
                Thread.sleep(3_000)
                if (running.isAlive) running.destroyForcibly()
            }
        }.apply { isDaemon = true }.start()
    }

    private fun startForegroundRuntime(projectName: String) {
        ContextCompat.startForegroundService(
            context,
            android.content.Intent(context, RuntimeExecutionService::class.java)
                .setAction(RuntimeExecutionService.ACTION_START)
                .putExtra(RuntimeExecutionService.EXTRA_PROJECT_NAME, projectName),
        )
    }

    private fun finishForegroundRuntime(completed: Boolean, projectName: String, detail: String) {
        // The first completion/failure post wins; later cleanup must not duplicate it.
        if (foregroundResultPosted) return
        foregroundResultPosted = true
        runCatching {
            context.startService(
                android.content.Intent(context, RuntimeExecutionService::class.java)
                    .setAction(
                        if (completed) RuntimeExecutionService.ACTION_COMPLETE
                        else RuntimeExecutionService.ACTION_FAILED,
                    )
                    .putExtra(RuntimeExecutionService.EXTRA_PROJECT_NAME, projectName)
                    .putExtra(RuntimeExecutionService.EXTRA_DETAIL, detail),
            )
        }.onFailure { error ->
            Log.w("PiBridge", "Could not post task result notification", error)
            context.stopService(android.content.Intent(context, RuntimeExecutionService::class.java))
        }
    }

    private fun cancelForegroundRuntime() {
        if (foregroundResultPosted) return
        foregroundResultPosted = true
        runCatching {
            context.startService(
                android.content.Intent(context, RuntimeExecutionService::class.java)
                    .setAction(RuntimeExecutionService.ACTION_CANCELLED),
            )
        }.onFailure {
            context.stopService(android.content.Intent(context, RuntimeExecutionService::class.java))
        }
    }

    private data class PendingPermission(val request: ToolRequest, val response: File)
    private class ProviderSessionException(message: String) : IllegalStateException(message)

    companion object {
        private const val MAX_DIFF_LINES = 2_000
        private const val MAX_RENDERED_DIFF_LINES = 600
        private const val DIFF_CONTEXT_LINES = 3
        private const val FOREGROUND_PROGRESS_MIN_INTERVAL_MS = 750L
    }
}

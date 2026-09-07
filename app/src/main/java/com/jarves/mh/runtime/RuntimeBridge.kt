package com.jarves.mh.runtime

import com.jarves.mh.model.ChatMessage
import com.jarves.mh.model.ChangeItem
import com.jarves.mh.model.ProviderProfile
import com.jarves.mh.model.ProjectKind
import com.jarves.mh.model.RuntimeEvent
import com.jarves.mh.model.ToolRequest
import kotlinx.coroutines.flow.Flow

data class RuntimeLaunchConfig(
    val executable: String,
    val arguments: List<String>,
    val environment: Map<String, String>,
)

interface RuntimeBridge {
    val events: Flow<RuntimeEvent>
    suspend fun startSession(projectId: String, projectSlug: String, projectKind: ProjectKind, prompt: String, conversationHistory: List<ChatMessage>, provider: ProviderProfile): String
    suspend fun respondToApproval(request: ToolRequest, approved: Boolean)
    suspend fun stopSession(sessionId: String)
    suspend fun stopActiveSession()
    suspend fun undoLastChanges(projectId: String): Boolean
    suspend fun acceptLastChanges(projectId: String)
    suspend fun loadPendingChanges(projectId: String): List<ChangeItem>
    suspend fun undoFileChange(projectId: String, path: String): Boolean
    suspend fun acceptFileChange(projectId: String, path: String): Boolean
}

object RuntimeLaunchConfigBuilder {
    /** Guest (PRoot) path of the Pi agent binary inside the Ubuntu rootfs. */
    const val PI_GUEST_PATH = "/usr/bin/pi"

    /**
     * Maps our provider kinds to Pi's native `--provider` ids.
     * Pi reads credentials from `--api-key` (or its own env/auth file),
     * so no provider base-URL plumbing is needed. CUSTOM has no dedicated
     * Pi provider: it runs against Pi's Anthropic provider with the user's
     * model and key; arbitrary base URLs require a Pi models.json (future).
     */
    fun piProviderId(profile: ProviderProfile): String = when (profile.kind) {
        com.jarves.mh.model.ProviderKind.ANTHROPIC -> "anthropic"
        com.jarves.mh.model.ProviderKind.LLM_ROUTER -> "openrouter"
        com.jarves.mh.model.ProviderKind.DEEPSEEK -> "deepseek"
        com.jarves.mh.model.ProviderKind.KIMI -> "kimi-coding"
        com.jarves.mh.model.ProviderKind.CUSTOM -> "anthropic"
    }

    private fun authEnvVar(piProviderId: String): String = when (piProviderId) {
        "openrouter" -> "OPENROUTER_API_KEY"
        "deepseek" -> "DEEPSEEK_API_KEY"
        "kimi-coding" -> "KIMI_API_KEY"
        "openai" -> "OPENAI_API_KEY"
        else -> "ANTHROPIC_API_KEY"
    }

    fun build(profile: ProviderProfile, authToken: String? = null, localGatewayUrl: String? = null): RuntimeLaunchConfig {
        val providerId = when (profile.kind.protocol) {
            com.jarves.mh.model.ProviderProtocol.OPENAI_RESPONSES,
            com.jarves.mh.model.ProviderProtocol.OPENAI_CHAT,
            -> "openai"
            else -> piProviderId(profile)
        }
        val model = profile.model.ifBlank { profile.kind.defaultModel }
        // Pi runs trusted by default (no permission gate); -a trusts
        // project-local files for the run. Prompt is appended positionally
        // by the caller after a "--" separator.
        val args = buildList {
            add("--mode")
            add("json")
            add("-p")
            add("--provider")
            add(providerId)
            if (model.isNotBlank()) {
                add("--model")
                add(model)
            }
            if (!authToken.isNullOrBlank()) {
                add("--api-key")
                add(authToken)
            }
            add("-a")
        }
        val environment = linkedMapOf(
            "DISABLE_AUTOUPDATER" to "1",
            "DISABLE_TELEMETRY" to "1",
            "PI_TELEMETRY" to "0",
        )
        // Auth travels via --api-key, but also export the provider's native
        // env var so a CLI-side flag rename can never silently break auth.
        if (!authToken.isNullOrBlank()) {
            environment[authEnvVar(providerId)] = authToken
        }
        return RuntimeLaunchConfig(
            executable = PI_GUEST_PATH,
            arguments = args,
            environment = environment,
        )
    }
}

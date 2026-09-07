package com.jarves.mh.runtime

import com.jarves.mh.model.ProviderKind
import com.jarves.mh.model.ProviderProfile
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class RuntimeLaunchConfigBuilderTest {
    @Test
    fun launchesBundledPiBinaryInJsonMode() {
        val config = RuntimeLaunchConfigBuilder.build(
            ProviderProfile(ProviderKind.ANTHROPIC, "https://api.anthropic.com", "model-a", true),
            authToken = "temporary-secret",
        )

        assertEquals("/usr/bin/pi", config.executable)
        assertTrue(config.arguments.contains("--mode"))
        assertTrue(config.arguments.contains("json"))
        assertTrue(config.arguments.contains("-p"))
        assertTrue(config.arguments.contains("--provider"))
        assertTrue(config.arguments.contains("anthropic"))
        assertTrue(config.arguments.contains("--model"))
        assertTrue(config.arguments.contains("model-a"))
        // Pi runs trusted: project-local files are approved for the run.
        assertTrue(config.arguments.contains("-a"))
        // The secret stays off argv (provider env var only) and -a is gone:
        // trust comes from root/.pi/agent/settings.json. Only
        // probe-verified flags (--mode/--provider/--model) ride along.
        assertFalse(config.arguments.contains("--api-key"))
        assertFalse(config.arguments.contains("-a"))
        assertEquals("temporary-secret", config.environment["ANTHROPIC_API_KEY"])
    }

    @Test
    fun mirrorsSecretIntoProviderNativeEnvVar() {
        val openRouter = RuntimeLaunchConfigBuilder.build(
            ProviderProfile(ProviderKind.LLM_ROUTER, "https://openrouter.ai/api", "model-a", true),
            authToken = "temporary-openrouter-secret",
        )
        assertEquals("temporary-openrouter-secret", openRouter.environment["OPENROUTER_API_KEY"])

        val kimi = RuntimeLaunchConfigBuilder.build(ProviderProfile(ProviderKind.KIMI), authToken = "k")
        assertEquals("k", kimi.environment["KIMI_API_KEY"])
    }

    @Test
    fun mapsProviderKindsToPiProviderIds() {
        assertEquals(
            "openrouter",
            RuntimeLaunchConfigBuilder.piProviderId(ProviderProfile(ProviderKind.LLM_ROUTER)),
        )
        assertEquals(
            "deepseek",
            RuntimeLaunchConfigBuilder.piProviderId(ProviderProfile(ProviderKind.DEEPSEEK)),
        )
        assertEquals(
            "kimi-coding",
            RuntimeLaunchConfigBuilder.piProviderId(ProviderProfile(ProviderKind.KIMI)),
        )
    }

    @Test
    fun disablesPiTelemetryAndAutoUpdate() {
        val config = RuntimeLaunchConfigBuilder.build(ProviderProfile(ProviderKind.ANTHROPIC))

        assertEquals("1", config.environment["DISABLE_AUTOUPDATER"])
        assertEquals("1", config.environment["DISABLE_TELEMETRY"])
        assertEquals("0", config.environment["PI_TELEMETRY"])
    }

    @Test
    fun openAiProtocolKindsUsePiOpenAiProvider() {
        val config = RuntimeLaunchConfigBuilder.build(
            ProviderProfile(ProviderKind.CUSTOM, "https://example.test/anthropic", "custom-model", true),
            authToken = "temporary-secret",
        )

        assertEquals("/usr/bin/pi", config.executable)
        assertTrue(config.arguments.contains("custom-model"))
        assertFalse(config.arguments.contains("--dangerously-skip-permissions"))
    }
}

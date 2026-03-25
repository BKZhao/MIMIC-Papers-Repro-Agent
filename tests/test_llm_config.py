from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from repro_agent.config import LLMConfig, load_pipeline_config  # noqa: E402
from repro_agent.llm import OpenAICompatibleClient  # noqa: E402


class LLMConfigEnvOverrideTests(unittest.TestCase):
    def test_env_overrides_llm_route_settings(self) -> None:
        config_path = ROOT / "configs" / "agentic.example.yaml"
        with patch.dict(
            os.environ,
            {
                "LLM_PROVIDER": "openai-compatible",
                "LLM_BASE_URL": "https://tokenx24.com/v1",
                "LLM_DEFAULT_MODEL": "custom-model-name",
                "LLM_API_KEY_ENV": "OPENAI_API_KEY",
                "LLM_TEMPERATURE": "0.2",
                "LLM_MAX_TOKENS": "1024",
                "LLM_TIMEOUT_SECONDS": "15",
                "LLM_ENABLED": "true",
            },
            clear=True,
        ):
            config = load_pipeline_config(config_path)

        self.assertEqual(config.llm.provider, "openai-compatible")
        self.assertEqual(config.llm.base_url, "https://tokenx24.com/v1")
        self.assertEqual(config.llm.default_model, "custom-model-name")
        self.assertEqual(config.llm.api_key_env, "OPENAI_API_KEY")
        self.assertEqual(config.llm.temperature, 0.2)
        self.assertEqual(config.llm.max_tokens, 1024)
        self.assertEqual(config.llm.timeout_seconds, 15)
        self.assertTrue(config.llm.enabled)

    def test_openai_api_key_is_auto_selected_when_explicit_env_name_is_absent(self) -> None:
        config_path = ROOT / "configs" / "agentic.example.yaml"
        with patch.dict(
            os.environ,
            {
                "OPENAI_API_KEY": "sk-test-openai-compatible",
            },
            clear=True,
        ):
            config = load_pipeline_config(config_path)

        self.assertEqual(config.llm.api_key_env, "OPENAI_API_KEY")


class OpenAICompatibleClientTests(unittest.TestCase):
    def test_client_reads_api_key_from_configured_env_var(self) -> None:
        client = OpenAICompatibleClient(
            LLMConfig(
                provider="openai-compatible",
                base_url="https://tokenx24.com/v1",
                default_model="custom-model-name",
                api_key_env="OPENAI_API_KEY",
            )
        )
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-openai-compatible"}, clear=True):
            self.assertTrue(client.is_enabled())
            self.assertEqual(client.api_key(), "sk-test-openai-compatible")


if __name__ == "__main__":
    unittest.main()

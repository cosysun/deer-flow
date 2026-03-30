"""Tests for user_id extraction in MemoryMiddleware."""

from unittest.mock import MagicMock, patch

import pytest

from deerflow.agents.middlewares.memory_middleware import MemoryMiddleware
from deerflow.config.memory_config import MemoryConfig


def _make_state(human_text="Hello", ai_text="Hi"):
    human = MagicMock(type="human", content=human_text)
    ai = MagicMock(type="ai", content=ai_text, tool_calls=[])
    return {"messages": [human, ai]}


def _make_runtime(context=None):
    runtime = MagicMock()
    runtime.context = context or {}
    return runtime


def _enabled_config():
    return MemoryConfig(enabled=True)


class TestMemoryMiddlewareUserIdExtraction:
    """MemoryMiddleware should extract user_id from config.configurable."""

    def test_extracts_user_id_from_configurable(self):
        """Should pass user_id from config.configurable to queue.add()."""
        state = _make_state()
        runtime = _make_runtime()

        mock_queue = MagicMock()

        with patch("deerflow.agents.middlewares.memory_middleware.get_memory_config", return_value=_enabled_config()):
            with patch("deerflow.agents.middlewares.memory_middleware.get_config", return_value={
                "configurable": {"thread_id": "t1", "user_id": "user-abc"}
            }):
                with patch("deerflow.agents.middlewares.memory_middleware.get_memory_queue", return_value=mock_queue):
                    middleware = MemoryMiddleware()
                    middleware.after_agent(state, runtime)

        mock_queue.add.assert_called_once()
        call_kwargs = mock_queue.add.call_args[1]
        assert call_kwargs["user_id"] == "user-abc"
        assert call_kwargs["thread_id"] == "t1"

    def test_user_id_defaults_to_none_when_not_in_configurable(self):
        """Should pass user_id=None to queue when not present in configurable."""
        state = _make_state()
        runtime = _make_runtime()

        mock_queue = MagicMock()

        with patch("deerflow.agents.middlewares.memory_middleware.get_memory_config", return_value=_enabled_config()):
            with patch("deerflow.agents.middlewares.memory_middleware.get_config", return_value={
                "configurable": {"thread_id": "t1"}
            }):
                with patch("deerflow.agents.middlewares.memory_middleware.get_memory_queue", return_value=mock_queue):
                    middleware = MemoryMiddleware()
                    middleware.after_agent(state, runtime)

        call_kwargs = mock_queue.add.call_args[1]
        assert call_kwargs["user_id"] is None

    def test_runtime_context_user_id_takes_priority_over_configurable(self):
        """runtime.context user_id should take priority over config.configurable user_id."""
        state = _make_state()
        runtime = _make_runtime(context={"thread_id": "t1", "user_id": "runtime-user"})

        mock_queue = MagicMock()

        with patch("deerflow.agents.middlewares.memory_middleware.get_memory_config", return_value=_enabled_config()):
            with patch("deerflow.agents.middlewares.memory_middleware.get_config", return_value={
                "configurable": {"thread_id": "t1", "user_id": "configurable-user"}
            }):
                with patch("deerflow.agents.middlewares.memory_middleware.get_memory_queue", return_value=mock_queue):
                    middleware = MemoryMiddleware()
                    middleware.after_agent(state, runtime)

        call_kwargs = mock_queue.add.call_args[1]
        assert call_kwargs["user_id"] == "runtime-user"

    def test_skips_when_memory_disabled(self):
        """Should not call queue.add when memory is disabled."""
        state = _make_state()
        runtime = _make_runtime()

        mock_queue = MagicMock()

        with patch("deerflow.agents.middlewares.memory_middleware.get_memory_config", return_value=MemoryConfig(enabled=False)):
            with patch("deerflow.agents.middlewares.memory_middleware.get_memory_queue", return_value=mock_queue):
                middleware = MemoryMiddleware()
                middleware.after_agent(state, runtime)

        mock_queue.add.assert_not_called()

    def test_skips_when_no_thread_id(self):
        """Should not call queue.add when thread_id is missing."""
        state = _make_state()
        runtime = _make_runtime()

        mock_queue = MagicMock()

        with patch("deerflow.agents.middlewares.memory_middleware.get_memory_config", return_value=_enabled_config()):
            with patch("deerflow.agents.middlewares.memory_middleware.get_config", return_value={"configurable": {}}):
                with patch("deerflow.agents.middlewares.memory_middleware.get_memory_queue", return_value=mock_queue):
                    middleware = MemoryMiddleware()
                    middleware.after_agent(state, runtime)

        mock_queue.add.assert_not_called()

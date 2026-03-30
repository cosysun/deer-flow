"""Tests for user_id propagation in MemoryUpdateQueue."""

from unittest.mock import MagicMock, patch

from deerflow.agents.memory.queue import ConversationContext, MemoryUpdateQueue
from deerflow.config.memory_config import MemoryConfig


def _enabled_config(**kwargs):
    return MemoryConfig(enabled=True, debounce_seconds=1, **kwargs)


class TestConversationContextUserIdField:
    """ConversationContext should carry user_id."""

    def test_default_user_id_is_none(self):
        ctx = ConversationContext(thread_id="t1", messages=[])
        assert ctx.user_id is None

    def test_user_id_is_stored(self):
        ctx = ConversationContext(thread_id="t1", messages=[], user_id="user-abc")
        assert ctx.user_id == "user-abc"


class TestMemoryUpdateQueueUserIdPropagation:
    """MemoryUpdateQueue should pass user_id to MemoryUpdater."""

    def test_add_stores_user_id_in_context(self):
        queue = MemoryUpdateQueue()
        with patch("deerflow.agents.memory.queue.get_memory_config", return_value=_enabled_config()):
            queue.add(thread_id="t1", messages=[], user_id="user-xyz")
        assert queue._queue[0].user_id == "user-xyz"
        if queue._timer:
            queue._timer.cancel()

    def test_add_without_user_id_defaults_to_none(self):
        queue = MemoryUpdateQueue()
        with patch("deerflow.agents.memory.queue.get_memory_config", return_value=_enabled_config()):
            queue.add(thread_id="t1", messages=[])
        assert queue._queue[0].user_id is None
        if queue._timer:
            queue._timer.cancel()

    def test_process_queue_passes_user_id_to_updater(self):
        queue = MemoryUpdateQueue()
        queue._queue = [
            ConversationContext(thread_id="t1", messages=[], user_id="user-123"),
        ]
        queue._processing = False

        mock_updater = MagicMock()
        mock_updater.update_memory.return_value = True

        with patch("deerflow.agents.memory.queue.MemoryUpdater", return_value=mock_updater):
            queue._process_queue()

        mock_updater.update_memory.assert_called_once_with(
            messages=[],
            thread_id="t1",
            agent_name=None,
            user_id="user-123",
        )

    def test_process_queue_passes_none_user_id_when_not_set(self):
        queue = MemoryUpdateQueue()
        queue._queue = [
            ConversationContext(thread_id="t1", messages=[]),
        ]
        queue._processing = False

        mock_updater = MagicMock()
        mock_updater.update_memory.return_value = True

        with patch("deerflow.agents.memory.queue.MemoryUpdater", return_value=mock_updater):
            queue._process_queue()

        mock_updater.update_memory.assert_called_once_with(
            messages=[],
            thread_id="t1",
            agent_name=None,
            user_id=None,
        )

    def test_deduplication_replaces_same_thread_regardless_of_user(self):
        """Same thread_id should be deduplicated even if user_id differs."""
        queue = MemoryUpdateQueue()
        with patch("deerflow.agents.memory.queue.get_memory_config", return_value=_enabled_config()):
            queue.add(thread_id="t1", messages=[], user_id="user-a")
            queue.add(thread_id="t1", messages=[], user_id="user-b")
        # Only the latest one should remain
        assert len(queue._queue) == 1
        assert queue._queue[0].user_id == "user-b"

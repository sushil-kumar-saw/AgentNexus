"""AgentNexus Kafka Event Streamer & Proactive Agent Bus.

This module provides a decoupled, non-blocking Kafka integration for AgentNexus:
1. Streams agent actions, observations, and rewards (Telemetry & Event Sourcing).
2. Streams task lifecycle events (Episode Reset, Brief Submission).
3. Streams anomaly / data-quality alerts for proactive multi-agent orchestration.

Dual-Mode Architecture:
- If a Kafka broker is reachable (configured via KAFKA_BOOTSTRAP_SERVERS), it uses real Kafka.
- If no broker is configured or reachable, it seamlessly falls back to a thread-safe
  file/in-memory buffer (`cache/kafka_events.jsonl`) so the entire application and tests
  work without hard infrastructure dependencies.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = REPO_ROOT / "cache"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_LOG = LOG_DIR / "kafka_events.jsonl"

# Topics
TOPIC_EPISODES = "agentnexus.episodes"
TOPIC_TELEMETRY = "agentnexus.telemetry"
TOPIC_ALERTS = "agentnexus.alerts"


class KafkaStreamer:
    """Enterprise event streamer with automatic real-Kafka / mock-fallback support."""

    def __init__(self, bootstrap_servers: Optional[str] = None) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.mode = "mock"
        self._producer = None
        self._lock = threading.Lock()
        self._event_history: List[Dict[str, Any]] = []
        self._history_limit = 100
        self._listeners: Dict[str, List[Callable[[Dict[str, Any]], None]]] = {}

        self._init_client()

    def _init_client(self) -> None:
        """Attempt connection to real Kafka if bootstrap_servers is specified."""
        if not self.bootstrap_servers:
            self.mode = "mock"
            return

        try:
            from kafka import KafkaProducer  # type: ignore

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                request_timeout_ms=2000,
                max_block_ms=2000,
                retries=2,
            )
            self.mode = "kafka"
            print(f"[KafkaStreamer] Connected to real Kafka broker at {self.bootstrap_servers}")
        except Exception as err:
            self.mode = "mock"
            self._producer = None
            print(f"[KafkaStreamer] Broker unavailable ({err}). Running in fallback mode.")

    def emit(self, topic: str, payload: Dict[str, Any], key: Optional[str] = None) -> Dict[str, Any]:
        """Emit a structured event to a Kafka topic or fallback log."""
        event = {
            "event_id": str(uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "topic": topic,
            "key": key,
            "data": payload,
        }

        # Keep recent events in-memory for live UI/API inspection
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._history_limit:
                self._event_history.pop(0)

        # Notify local in-process consumers/listeners (if any registered)
        if topic in self._listeners:
            for cb in self._listeners[topic]:
                try:
                    cb(event)
                except Exception as e:
                    print(f"[KafkaStreamer] Listener callback error: {e}")

        # Publish to real Kafka or write to file fallback
        if self.mode == "kafka" and self._producer is not None:
            try:
                self._producer.send(topic, key=key, value=event)
            except Exception as e:
                print(f"[KafkaStreamer] Error publishing to Kafka: {e}")
                self._append_to_fallback_file(event)
        else:
            self._append_to_fallback_file(event)

        return event

    def _append_to_fallback_file(self, event: Dict[str, Any]) -> None:
        try:
            with open(FALLBACK_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(event) + "\n")
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # High-level domain helpers (Easy to use in AgentNexus)
    # -------------------------------------------------------------------------

    def emit_reset(self, episode_id: str, task: str, use_rag: bool) -> Dict[str, Any]:
        """Publish episode started event."""
        return self.emit(
            topic=TOPIC_EPISODES,
            key=episode_id,
            payload={
                "event_type": "EPISODE_RESET",
                "episode_id": episode_id,
                "task": task,
                "use_rag": use_rag,
            },
        )

    def emit_step(
        self,
        episode_id: str,
        step_count: int,
        action: Dict[str, Any],
        reward: float,
        done: bool,
        consulted_experts: List[str],
        data_quality_score: float,
        issues: List[str],
        terminal_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Publish step telemetry event."""
        return self.emit(
            topic=TOPIC_TELEMETRY,
            key=episode_id,
            payload={
                "event_type": "STEP_EXECUTED",
                "episode_id": episode_id,
                "step_count": step_count,
                "action": action,
                "reward": reward,
                "done": done,
                "consulted_experts": consulted_experts,
                "data_quality_score": data_quality_score,
                "issues": issues,
                "terminal_score": terminal_score,
            },
        )

    def emit_alert(
        self,
        source: str,
        alert_type: str,
        severity: str,
        details: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Publish a proactive anomaly/risk alert."""
        return self.emit(
            topic=TOPIC_ALERTS,
            key=source,
            payload={
                "event_type": "PROACTIVE_ALERT",
                "source": source,
                "alert_type": alert_type,
                "severity": severity,
                "details": details,
            },
        )

    # -------------------------------------------------------------------------
    # Subscription / Consumer utilities
    # -------------------------------------------------------------------------

    def register_listener(self, topic: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for events on a specific topic."""
        with self._lock:
            if topic not in self._listeners:
                self._listeners[topic] = []
            self._listeners[topic].append(callback)

    def get_recent_events(self, limit: int = 20, topic: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve recent events (useful for /kafka/events endpoint)."""
        with self._lock:
            events = list(self._event_history)
        if topic:
            events = [e for e in events if e.get("topic") == topic]
        return events[-limit:]

    def status(self) -> Dict[str, Any]:
        """Get current streamer diagnostics."""
        with self._lock:
            count = len(self._event_history)
        return {
            "mode": self.mode,
            "bootstrap_servers": self.bootstrap_servers or "None (Running mock/fallback)",
            "buffered_events_count": count,
            "topics": [TOPIC_EPISODES, TOPIC_TELEMETRY, TOPIC_ALERTS],
            "fallback_log_path": str(FALLBACK_LOG),
        }

    def close(self) -> None:
        """Close producer connection cleanly."""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
            except Exception:
                pass


# Global singleton instance
streamer = KafkaStreamer()

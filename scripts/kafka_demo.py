#!/usr/bin/env python3
"""Interactive demo script showing Kafka streaming & proactive alerting in AgentNexus.

Usage:
    python3 scripts/kafka_demo.py

Features demonstrated:
1. Producer publishing Episode Reset & Step Telemetry events.
2. In-memory / Real Kafka streaming across three dedicated topics:
   - agentnexus.episodes
   - agentnexus.telemetry
   - agentnexus.alerts
3. Proactive Event-Driven Anomaly Consumer:
   - Simulates a Data Analyst anomaly alert on `agentnexus.alerts`
   - Triggers the Chief of Staff policy to proactively respond!
"""

import json
import sys
import time
from pathlib import Path

# Add project root to path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ceo_brief_env.environment import CEOBriefEnvironment, oracle_action_for_observation
from kafka_streamer import (
    TOPIC_ALERTS,
    TOPIC_EPISODES,
    TOPIC_TELEMETRY,
    streamer,
)


def print_header(title: str) -> None:
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def alert_consumer_callback(event: dict) -> None:
    """Consumer callback simulating an autonomous reactive agent."""
    data = event.get("data", {})
    alert_type = data.get("alert_type")
    severity = data.get("severity")
    print(f"\n🚨 [KAFKA ALERT CONSUMER] Intercepted event on {event.get('topic')}:")
    print(f"   Severity: {severity} | Type: {alert_type}")
    print(f"   Details : {data.get('details')}")
    print("   👉 [ACTION TRIGGERED] Chief of Staff autonomously scheduling Risk Brief investigation!\n")


def main() -> None:
    print_header("AgentNexus: Apache Kafka Event Bus & Streaming Demo")

    # 1. Display Broker Status
    status = streamer.status()
    print(f"[*] Kafka Streamer Mode: {status['mode'].upper()}")
    print(f"[*] Bootstrap Servers  : {status['bootstrap_servers']}")
    print(f"[*] Monitored Topics   : {', '.join(status['topics'])}")
    print(f"[*] Fallback Log File  : {status['fallback_log_path']}")

    # 2. Register Consumer Listener on Alerts Topic
    print("\n[+] Registering Proactive Anomaly Consumer on topic:", TOPIC_ALERTS)
    streamer.register_listener(TOPIC_ALERTS, alert_consumer_callback)

    # 3. Simulate an Agent Episode with Kafka Telemetry
    print_header("1. Running Multi-Agent Episode with Kafka Telemetry")
    env = CEOBriefEnvironment()
    obs = env.reset("easy_brief")

    # Publish reset
    reset_event = streamer.emit_reset(
        episode_id=env.episode_id,
        task=env.task_name,
        use_rag=False,
    )
    print(f"📤 [PRODUCED -> {TOPIC_EPISODES}] EPISODE_RESET (id: {env.episode_id[:8]}...)")

    step_count = 0
    while not obs.done and step_count < 10:
        action = oracle_action_for_observation(obs)
        action_dict = action.model_dump(exclude_none=True)
        obs = env.step(action)
        step_count += 1

        # Publish step telemetry
        step_event = streamer.emit_step(
            episode_id=env.episode_id,
            step_count=step_count,
            action=action_dict,
            reward=obs.reward,
            done=obs.done,
            consulted_experts=obs.consulted_experts,
            data_quality_score=obs.data_quality_score,
            issues=obs.issues,
            terminal_score=obs.terminal_grader_score,
        )
        print(
            f"📤 [PRODUCED -> {TOPIC_TELEMETRY}] "
            f"Step {step_count} | Action: {action.action_type}:{action.expert_id} | "
            f"Reward: {obs.reward:+.4f} | Done: {obs.done}"
        )

    print(f"\n[✓] Episode completed. Terminal Score: {obs.terminal_grader_score:.4f}")

    # 4. Simulate a Proactive Anomaly Trigger
    print_header("2. Proactive Alerting via Kafka Event Stream")
    print("[*] Simulating live stream anomaly detection (e.g. Data Quality drop)...")
    time.sleep(0.5)

    streamer.emit_alert(
        source="data_quality_stream_monitor",
        alert_type="CORRUPT_TRANSACTION_BURST",
        severity="HIGH",
        details={
            "detected_null_rate": 0.42,
            "threshold": 0.15,
            "impacted_system": "SalesOrderIngestion",
        },
    )

    # 5. Summary of Recent Events
    print_header("3. Summary of Streamed Events")
    recent = streamer.get_recent_events(limit=10)
    print(f"Total events captured in current session: {len(recent)}")
    for i, ev in enumerate(recent, 1):
        print(f" {i:2d}. [{ev['timestamp'][11:19]}] Topic: {ev['topic']:<22} | Type: {ev['data'].get('event_type')}")

    print("\n[PASS] Kafka integration demonstration completed successfully.")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()

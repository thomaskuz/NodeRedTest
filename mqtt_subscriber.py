"""Simple MQTT subscriber for the local HiveMQ broker."""

from __future__ import annotations

import argparse
import uuid

import paho.mqtt.client as mqtt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--topic", default="demo/test", help="Topic filter to subscribe to")
    parser.add_argument(
        "--client-id",
        default=None,
        help="Optional MQTT client id (random if omitted)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after receiving this many messages (0 keeps looping)",
    )
    return parser.parse_args()


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None) -> None:  # type: ignore[override]
    print(f"Connected. Subscribing to '{userdata['topic']}'")
    client.subscribe(userdata["topic"])


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
    print(f"[{msg.topic}] {msg.payload.decode('utf-8', errors='replace')}")
    userdata["received"] += 1
    max_messages = userdata["max_messages"]
    if max_messages and userdata["received"] >= max_messages:
        print("Reached message limit. Disconnecting…")
        client.disconnect()


def main() -> None:
    args = parse_args()
    userdata = {"topic": args.topic, "received": 0, "max_messages": args.max_messages}

    client_id = args.client_id or f"mqtt-subscriber-{uuid.uuid4().hex[:8]}"
    client = mqtt.Client(client_id=client_id, userdata=userdata)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.host, args.port, keepalive=60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("Stopping subscriber…")
        client.disconnect()


if __name__ == "__main__":
    main()

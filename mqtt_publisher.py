"""Simple MQTT publisher for the local HiveMQ broker."""

from __future__ import annotations

import argparse
import time
import uuid

import paho.mqtt.client as mqtt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--topic", default="demo/test", help="Topic to publish to")
    parser.add_argument(
        "--message",
        default="Hello from Codex!",
        help="Message payload. Use {n} to include the message number when publishing multiple messages.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of messages to publish (0 keeps publishing until interrupted)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between messages",
    )
    parser.add_argument(
        "--client-id",
        default=None,
        help="Optional MQTT client id (random if omitted)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    client_id = args.client_id or f"mqtt-publisher-{uuid.uuid4().hex[:8]}"
    client = mqtt.Client(client_id=client_id)

    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    msg_num = 0
    try:
        while True:
            if args.count and msg_num >= args.count:
                break

            payload = args.message.format(n=msg_num + 1)
            info = client.publish(args.topic, payload=payload, qos=0, retain=False)
            info.wait_for_publish()
            print(f"Published '{payload}' to topic '{args.topic}'")
            msg_num += 1

            if args.count and msg_num >= args.count:
                break
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("Publishing interrupted by user.")

    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()

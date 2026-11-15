"""Coinbase trades -> Kafka topic "coinbase_trades".

  python producer.py        # PUSH: every trade ("match") of six products, from the Coinbase WebSocket

Adapted from the class script quakes_producer.py (Mon 02 Nov).
The connection code follows probe.py from the Project 3 kit.
"""
import asyncio
import json
import logging

from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "coinbase_trades"  # part 1: the name on my card
WSS_URL = "wss://ws-feed.exchange.coinbase.com"
SUBSCRIBE = {"type": "subscribe",
             "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD",
                             "XRP-USD", "DOGE-USD", "ADA-USD"],
             "channels": ["matches"]}


def send(producer, message):
    """Part 2: one Kafka record per message, keyed as on my card."""
    key = message["data"].get("product_id")  # .get(): no crash if missing
    if key is None:
        logging.warning("No product_id: sent without a key")
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))


async def live(producer):
    """Part 3: PUSH over a WebSocket, with a reconnect loop."""
    sent = 0
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            if SUBSCRIBE:  # Coinbase closes the connection after 5 s without it
                await ws.write_message(json.dumps(SUBSCRIBE))
            logging.info("Connected. Waiting for trades...")
            while True:
                text = await ws.read_message()
                if text is None:
                    break  # the server closed the connection
                try:
                    send(producer, {"action": "live", "data": json.loads(text)})
                    sent += 1
                    if sent % 1000 == 0:  # part 4: one line every 1,000 messages
                        logging.info("%d messages sent to %s", sent, TOPIC)
                except (ValueError, KeyError, TypeError) as e:
                    logging.warning("Skipped a message: %r", e)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)


def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass

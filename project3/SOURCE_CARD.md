# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:** Mani Ferdosian
**Date:** Sun 15 Nov 2026

## 1. The source
- Source and URL: Coinbase Exchange trades (`matches` channel), `wss://ws-feed.exchange.coinbase.com`
- Method (WebSocket push, REST polling or REST history pull): WebSocket, push, like `live()` on Mon 02 Nov
- Account, key or User-Agent needed: none; a subscribe message within 5 seconds of connecting
- Limits I know about (rate limits, paging, reconnects): messages can be dropped and the connection can close; reconnect after a pause. Quieter at night.

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{"type": "last_match", "trade_id": 855545505,
 "maker_order_id": "651669ef-8230-4543-9d2c-649c04a3a0f2",
 "taker_order_id": "2616c461-443f-4ef1-b002-529fae67b2df",
 "side": "sell", "size": "0.2766324", "price": "1808.08",
 "product_id": "ETH-USD", "sequence": 105176547084,
 "time": "2026-11-15T17:43:54.742560Z"}
```

## 3. Volume maths
- Measured rate (from the probe): 61.11 messages per second (3,667 in 60 s, Sun 15 Nov ~17:44 UTC, 6 products)
- Live capture needed for 50,000: about 818 seconds (14 min) at this rate
- History / backfill available? How many days? No, the WebSocket has no replay. Live only.
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): run the producer for at least 1 hour this evening. At 61/s that is about 220,000; even at a quiet 5.7/s (SOURCES.md) 3 hours gives about 60,000.

## 4. Topic design
- Topic name: coinbase_trades (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: product_id
- Why this key (ordering, spread across partitions): all trades of one product stay in order in one partition, so buys and sells of BTC-USD can be counted per minute in order. Six products spread over the 3 partitions.

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) the same partition + offset
- What makes two records **the same thing** (the same trade, post, aircraft position)? the same product_id + trade_id (a `last_match` after a reconnect can repeat a trade I already have)
- If the same thing can arrive in several versions, which one do I keep? a trade does not change, so all copies are equal: keep the first one (lowest offset)

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: per product, in 1-minute windows, what share of trades are buys vs sells, and does that share move with the price?

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join

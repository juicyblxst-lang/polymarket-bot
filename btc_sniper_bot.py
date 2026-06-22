#!/usr/bin/env python3
"""
Polymarket 5-Minute BTC Sniper – WebSocket + Binance Edge
Includes Flask web server for Render free tier compatibility.
"""

import asyncio
import json
import time
import logging
from collections import deque
from threading import Thread

import websockets
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL
from telegram import Bot
from flask import Flask

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

TELEGRAM_TOKEN = "8949632376:AAH4u0PCOvO_oi5ygnmlEyNuVx2DDMyalFU"
TELEGRAM_CHAT_ID = "8579537304"
PRIVATE_KEY = "0xc920d644c816f7ab1894603904475342d1559d0ce72d848f7142bebf80aa44da"
CLOB_API_KEY = "019edc92-98b6-7cb2-999e-db5ee9de17e3"
CLOB_SECRET = "5ET5zdC3YnBRRjy0hMoSHwY2E0i9Zc4_gFvxCKn89ko="
CLOB_PASSPHRASE = "6b00ee54064057d5f35033ac5bf2d0ec9a536919562cd711942dad0945ef9c80"

MARKET_TOKEN_ID = "100824312187318380959439588742678408851068029073889761718679633905962383561267"
PRICE_THRESHOLD = 0.003          # 0.3% move
MIN_POSITION_SIZE = 5
BID_ASK_SPREAD = 0.01

POLY_WS_URL = "wss://ws-subscriptions-frontend-clob.polymarket.com/ws/market"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# FLASK WEB SERVER (Keeps Render happy)
# ============================================================

app = Flask(__name__)

@app.route('/')
def health_check():
    return "Bot is running!", 200

@app.route('/health')
def health():
    return "OK", 200

def run_webserver():
    app.run(host='0.0.0.0', port=10000)

# ============================================================
# TELEGRAM NOTIFIER
# ============================================================

class TelegramNotifier:
    def __init__(self, token, chat_id):
        self.bot = Bot(token=token)
        self.chat_id = chat_id

    async def send(self, message):
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Telegram error: {e}")

notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

# ============================================================
# POLYMARKET ORDER BOOK
# ============================================================

class PolymarketOrderBook:
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.mid_price = None
        self.last_update = 0

    def apply_snapshot(self, msg):
        self.bids = {float(b["price"]): float(b["size"]) for b in msg.get("bids", [])}
        self.asks = {float(a["price"]): float(a["size"]) for a in msg.get("asks", [])}
        self.last_update = time.time()
        self._update_mid()

    def apply_delta(self, msg):
        changes = msg.get("changes", [])
        for ch in changes:
            side = self.bids if ch.get("side") == "BUY" else self.asks
            price = float(ch["price"])
            size = float(ch["size"])
            if size == 0:
                side.pop(price, None)
            else:
                side[price] = size
        self.last_update = time.time()
        self._update_mid()

    def _update_mid(self):
        bid = max(self.bids.keys()) if self.bids else None
        ask = min(self.asks.keys()) if self.asks else None
        if bid and ask:
            self.mid_price = (bid + ask) / 2
        else:
            self.mid_price = None

    def get_price(self):
        return self.mid_price

# ============================================================
# POLYMARKET EXECUTOR
# ============================================================

class PolymarketExecutor:
    def __init__(self, token_id):
        self.token_id = token_id
        self.client = ClobClient(
            host="https://clob.polymarket.com",
            key=PRIVATE_KEY,
            chain_id=137,
            signature_type=1,
            creds={
                "api_key": CLOB_API_KEY,
                "secret": CLOB_SECRET,
                "passphrase": CLOB_PASSPHRASE,
            }
        )

    async def place_order(self, side, price, size):
        try:
            order_args = OrderArgs(
                token_id=self.token_id,
                side=side,
                price=price,
                size=size,
                fee_rate_bps=0,
            )
            resp = self.client.create_order(order_args)
            logger.info(f"✅ Order placed: {side} ${price:.3f} x {size}")
            await notifier.send(f"✅ Order placed: {side} ${price:.3f} x {size}")
            return resp
        except Exception as e:
            logger.error(f"Order failed: {e}")
            await notifier.send(f"❌ Order failed: {e}")
            return None

    async def execute_signal(self, direction, mid):
        if mid is None:
            return
        if direction == "UP":
            buy_price = min(mid + BID_ASK_SPREAD, 0.99)
            await self.place_order(BUY, buy_price, MIN_POSITION_SIZE)
        else:
            sell_price = max(mid - BID_ASK_SPREAD, 0.01)
            await self.place_order(SELL, sell_price, MIN_POSITION_SIZE)

# ============================================================
# BINANCE PRICE FEED (Edge)
# ============================================================

class BinancePriceFeed:
    def __init__(self, threshold=0.003, window_seconds=60):
        self.threshold = threshold
        self.window_seconds = window_seconds
        self.price_window = deque(maxlen=1000)
        self.last_signal_time = 0

    async def run(self):
        while True:
            try:
                async with websockets.connect(BINANCE_WS_URL) as ws:
                    logger.info("🔗 Binance WebSocket connected")
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                            if "p" in data:
                                price = float(data["p"])
                                self.price_window.append((time.time(), price))
                                self._check_signal()
                        except json.JSONDecodeError:
                            pass
            except Exception as e:
                logger.error(f"Binance WS error: {e}")
                await asyncio.sleep(5)

    def _check_signal(self):
        if len(self.price_window) < 2:
            return
        now = time.time()
        cutoff = now - self.window_seconds
        while self.price_window and self.price_window[0][0] < cutoff:
            self.price_window.popleft()
        if len(self.price_window) < 2:
            return
        oldest = self.price_window[0][1]
        latest = self.price_window[-1][1]
        pct_move = (latest - oldest) / oldest
        if abs(pct_move) >= self.threshold and (now - self.last_signal_time) > 30:
            direction = "UP" if pct_move > 0 else "DOWN"
            logger.info(f"🚨 Binance signal: {direction} {pct_move*100:.2f}%")
            self.last_signal_time = now
            asyncio.create_task(notifier.send(f"🚨 Binance signal: BTC {direction} {pct_move*100:.2f}%"))

# ============================================================
# POLYMARKET WEBSOCKET STREAM
# ============================================================

async def stream_order_book(token_id, book, executor):
    while True:
        try:
            async with websockets.connect(POLY_WS_URL) as ws:
                logger.info("🔗 Polymarket WebSocket connected")
                subscribe = {"assets_ids": [token_id], "type": "market"}
                await ws.send(json.dumps(subscribe))

                price_history = deque(maxlen=10)

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        if isinstance(data, list):
                            for msg in data:
                                event_type = msg.get("event_type") or msg.get("type")
                                if event_type in ("book", "snapshot"):
                                    book.apply_snapshot(msg)
                                elif event_type in ("price_change", "delta"):
                                    book.apply_delta(msg)
                                    mid = book.get_price()
                                    if mid:
                                        price_history.append(mid)
                                        if len(price_history) >= 2 and (time.time() - book.last_update) < 2:
                                            latest = price_history[-1]
                                            oldest = price_history[0]
                                            pct_move = (latest - oldest) / oldest if oldest else 0
                                            if abs(pct_move) >= PRICE_THRESHOLD:
                                                direction = "UP" if pct_move > 0 else "DOWN"
                                                logger.info(f"🚨 Polymarket signal: {direction} {pct_move*100:.2f}%")
                                                await executor.execute_signal(direction, mid)
                                                price_history.clear()
                        else:
                            event_type = data.get("event_type") or data.get("type")
                            if event_type in ("book", "snapshot"):
                                book.apply_snapshot(data)
                            elif event_type in ("price_change", "delta"):
                                book.apply_delta(data)
                                mid = book.get_price()
                                if mid:
                                    price_history.append(mid)
                                    if len(price_history) >= 2 and (time.time() - book.last_update) < 2:
                                        latest = price_history[-1]
                                        oldest = price_history[0]
                                        pct_move = (latest - oldest) / oldest if oldest else 0
                                        if abs(pct_move) >= PRICE_THRESHOLD:
                                            direction = "UP" if pct_move > 0 else "DOWN"
                                            logger.info(f"🚨 Polymarket signal: {direction} {pct_move*100:.2f}%")
                                            await executor.execute_signal(direction, mid)
                                            price_history.clear()

                    except json.JSONDecodeError:
                        pass

        except Exception as e:
            logger.error(f"Polymarket WS error: {e}")
            await asyncio.sleep(5)

# ============================================================
# MAIN
# ============================================================

async def main():
    logger.info("🚀 BTC Sniper (Polymarket + Binance Edge)")
    await notifier.send("🚀 BTC Sniper started – Polymarket WebSocket + Binance edge")

    # Start web server in background thread
    Thread(target=run_webserver, daemon=True).start()
    logger.info("🌐 Web server started on port 10000")

    book = PolymarketOrderBook()
    executor = PolymarketExecutor(MARKET_TOKEN_ID)
    binance_feed = BinancePriceFeed()

    try:
        await asyncio.gather(
            stream_order_book(MARKET_TOKEN_ID, book, executor),
            binance_feed.run()
        )
    except KeyboardInterrupt:
        logger.info("Bot stopped")
        await notifier.send("🛑 Bot stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")

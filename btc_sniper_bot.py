#!/usr/bin/env python3
import asyncio
import json
import time
import logging
from collections import deque

import websockets
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL
from telegram import Bot

load_dotenv()

TELEGRAM_TOKEN = "8949632376:AAH4u0PCOvO_oi5ygnmlEyNuVx2DDMyalFU"
TELEGRAM_CHAT_ID = "8579537304"
PRIVATE_KEY = "0xc920d644c816f7ab1894603904475342d1559d0ce72d848f7142bebf80aa44da"
CLOB_API_KEY = "019edc92-98b6-7cb2-999e-db5ee9de17e3"
CLOB_SECRET = "5ET5zdC3YnBRRjy0hMoSHwY2E0i9Zc4_gFvxCKn89ko="
CLOB_PASSPHRASE = "6b00ee54064057d5f35033ac5bf2d0ec9a536919562cd711942dad0945ef9c80"

MARKET_TOKEN_ID = "100824312187318380959439588742678408851068029073889761718679633905962383561267"
PRICE_THRESHOLD = 0.003
MIN_POSITION_SIZE = 5
BID_ASK_SPREAD = 0.01
WS_URL = "wss://ws-subscriptions-frontend-clob.polymarket.com/ws/market"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

async def stream_order_book(token_id, book, executor):
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                logger.info("Connected to Polymarket WebSocket")
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
                                                logger.info(f"🚨 Signal: {direction} {pct_move*100:.2f}%")
                                                await notifier.send(f"🚨 Signal: BTC {direction} {pct_move*100:.2f}% @ ${latest:.4f}")
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
                                            logger.info(f"🚨 Signal: {direction} {pct_move*100:.2f}%")
                                            await notifier.send(f"🚨 Signal: BTC {direction} {pct_move*100:.2f}% @ ${latest:.4f}")
                                            await executor.execute_signal(direction, mid)
                                            price_history.clear()

                    except json.JSONDecodeError:
                        pass

        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"Disconnected: {e}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"WS error: {e}")
            await asyncio.sleep(5)

async def main():
    logger.info("🚀 BTC Sniper (Polymarket WebSocket Edition)")
    await notifier.send("🚀 BTC Sniper started – using Polymarket WebSocket")

    book = PolymarketOrderBook()
    executor = PolymarketExecutor(MARKET_TOKEN_ID)

    try:
        await stream_order_book(MARKET_TOKEN_ID, book, executor)
    except KeyboardInterrupt:
        logger.info("Bot stopped")
        await notifier.send("🛑 Bot stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")

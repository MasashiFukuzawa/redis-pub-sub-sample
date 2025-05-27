#!/usr/bin/env python3
"""
Redis List + Pub/Sub の実践的なサンプル実装
リアルタイムメッセージングシステムの主要機能を実装
"""

import asyncio
import json
import redis.asyncio as redis
from datetime import datetime
from typing import List, Dict, Any

# Redis接続設定
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# セッションID（処理単位を識別するID）
SESSION_ID = "session_123"

# Redisキーとチャネルの定義
MESSAGES_LIST_KEY = f"session:{SESSION_ID}:messages"  # メッセージ履歴保存用
NEW_MESSAGE_CHANNEL = f"session:{SESSION_ID}:notify"  # 新着通知用
CONTROL_CHANNEL = f"session:{SESSION_ID}:control"  # 制御シグナル用


class MessageProducer:
    """メッセージ生成・送信側（バックグラウンド処理を想定）"""

    def __init__(self):
        self.redis_client = None
        self.running = True

    async def connect(self):
        """Redis接続"""
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        await self.redis_client.ping()
        print("✅ Producer: Redisに接続しました")

    async def send_message(self, content: str):
        """メッセージを送信"""
        message = {
            "id": datetime.now().isoformat(),
            "content": content,
            "type": "message",
        }

        # 1. Listに保存（永続化）
        message_json = json.dumps(message)
        await self.redis_client.rpush(MESSAGES_LIST_KEY, message_json)
        print(f"💾 Producer: メッセージを永続化: {content}")

        # 2. 新着通知を発行
        await self.redis_client.publish(NEW_MESSAGE_CHANNEL, "new")
        print("📢 Producer: 新着通知を発行")

    async def listen_for_stop(self):
        """停止シグナルを監視"""
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(CONTROL_CHANNEL)

        async for message in pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                if data == "STOP":
                    print("🛑 Producer: 停止シグナルを受信")
                    self.running = False
                    break

        await pubsub.unsubscribe()
        await pubsub.aclose()

    async def run(self):
        """メッセージ送信のメインループ"""
        await self.connect()

        # 停止シグナル監視を開始
        stop_task = asyncio.create_task(self.listen_for_stop())

        # 定期的にメッセージを送信
        messages = [
            "こんにちは！",
            "Redisのテストです",
            "List + Pub/Subパターンを実装しています",
            "リアルタイムで受信できていますか？",
            "最後のメッセージです",
        ]

        for i, msg in enumerate(messages):
            if not self.running:
                break

            await self.send_message(f"[{i + 1}] {msg}")
            await asyncio.sleep(2)  # 2秒間隔で送信

        # 完了メッセージ
        if self.running:
            completion_msg = {
                "id": datetime.now().isoformat(),
                "type": "status",
                "status": "completed",
            }
            await self.redis_client.rpush(MESSAGES_LIST_KEY, json.dumps(completion_msg))
            await self.redis_client.publish(NEW_MESSAGE_CHANNEL, "new")
            print("✅ Producer: 全メッセージ送信完了")

        stop_task.cancel()
        await self.redis_client.aclose()


class MessageConsumer:
    """メッセージ受信・配信側（ストリーミングAPIを想定）"""

    def __init__(self):
        self.redis_client = None
        self.last_index = -1

    async def connect(self):
        """Redis接続"""
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        await self.redis_client.ping()
        print("✅ Consumer: Redisに接続しました")

    async def get_initial_messages(self) -> List[Dict[str, Any]]:
        """既存メッセージを取得"""
        messages_json = await self.redis_client.lrange(MESSAGES_LIST_KEY, 0, -1)
        messages = [json.loads(msg) for msg in messages_json]
        self.last_index = len(messages) - 1
        return messages

    async def stream_messages(self):
        """メッセージをストリーミング"""
        await self.connect()

        # 1. 既存メッセージを取得して表示
        print("\n📥 Consumer: 既存メッセージを取得中...")
        initial_messages = await self.get_initial_messages()
        for msg in initial_messages:
            if msg["type"] == "message":
                print(f"  既存: {msg['content']}")

        if initial_messages:
            print(f"  → {len(initial_messages)}件の既存メッセージを取得しました\n")

        # 2. Pub/Subで新着を監視
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(NEW_MESSAGE_CHANNEL)
        print("👂 Consumer: 新着メッセージを待機中...\n")

        async for notification in pubsub.listen():
            if notification["type"] == "message" and notification["data"] == "new":
                # 3. 新着メッセージを取得
                new_messages_json = await self.redis_client.lrange(
                    MESSAGES_LIST_KEY, self.last_index + 1, -1
                )

                new_messages = [json.loads(msg) for msg in new_messages_json]
                self.last_index += len(new_messages)

                # 4. 新着メッセージを表示
                for msg in new_messages:
                    if msg["type"] == "message":
                        print(f"🆕 新着: {msg['content']}")
                    elif msg["type"] == "status" and msg["status"] == "completed":
                        print("✅ Consumer: ストリーム完了")
                        await pubsub.unsubscribe()
                        await pubsub.aclose()
                        await self.redis_client.aclose()
                        return


class SessionController:
    """セッション制御（停止・管理機能）"""

    @staticmethod
    async def stop_session():
        """セッションを停止"""
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        await client.publish(CONTROL_CHANNEL, "STOP")
        print("🛑 Controller: 停止シグナルを送信")
        await client.aclose()


async def cleanup():
    """Redisのクリーンアップ"""
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    await client.delete(MESSAGES_LIST_KEY)
    print("🧹 Cleanup: Redisキーを削除しました")
    await client.aclose()


async def demo_normal_flow():
    """通常フローのデモ"""
    print("=== 通常フローのデモ ===\n")

    # クリーンアップ
    await cleanup()

    # Producer と Consumer を同時に実行
    producer = MessageProducer()
    consumer = MessageConsumer()

    await asyncio.gather(producer.run(), consumer.stream_messages())


async def demo_with_stop():
    """途中停止のデモ"""
    print("\n\n=== 途中停止のデモ ===\n")

    # クリーンアップ
    await cleanup()

    producer = MessageProducer()
    consumer = MessageConsumer()

    # Producer と Consumer を開始
    producer_task = asyncio.create_task(producer.run())
    consumer_task = asyncio.create_task(consumer.stream_messages())

    # 3秒後に停止
    await asyncio.sleep(3)
    await SessionController.stop_session()

    # タスクの完了を待つ
    await producer_task
    consumer_task.cancel()


async def demo_reconnect():
    """再接続のデモ（Listの永続性を確認）"""
    print("\n\n=== 再接続のデモ ===\n")

    # クリーンアップ
    await cleanup()

    # 先にProducerのみ実行
    print("1️⃣ Producerのみ実行（Consumerは未接続）\n")
    producer = MessageProducer()
    await producer.run()

    # 少し待つ
    await asyncio.sleep(2)

    # 後からConsumerが接続
    print("\n2️⃣ Consumerが後から接続\n")
    consumer = MessageConsumer()
    await consumer.stream_messages()

    print("\n✅ Listに保存されていたため、全メッセージを受信できました！")


async def main():
    """メイン関数"""
    print("Redis List + Pub/Sub 実践サンプル\n")
    print("リアルタイムメッセージングシステムの実装")
    print("=" * 50)

    # 各デモを実行
    await demo_normal_flow()
    await demo_with_stop()
    await demo_reconnect()

    # 最後にクリーンアップ
    await cleanup()
    print("\n✅ すべてのデモが完了しました")


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Redis List + Pub/Sub の最小限のサンプル
リアルタイムメッセージングの基本パターンを実装
"""

import asyncio
import json
import os
import redis.asyncio as redis

# Redis接続（環境変数から設定を取得）
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# キーとチャネルの定義
SESSION_ID = "session_123"
MESSAGE_LIST = f"messages:{SESSION_ID}"  # メッセージ履歴の保存用
NOTIFICATION_CHANNEL = f"notify:{SESSION_ID}"  # 新着通知用


async def producer():
    """Producer: メッセージを生成・送信する側"""
    print("📤 Producer: 開始")

    messages = ["処理を開始します", "データを分析中...", "完了しました"]

    for msg in messages:
        # 1. メッセージをListに永続化
        message_data = {"type": "message", "content": msg}
        await client.rpush(MESSAGE_LIST, json.dumps(message_data))

        # 2. 新着があることを通知
        await client.publish(NOTIFICATION_CHANNEL, "new")

        print(f"   送信 → {msg}")
        await asyncio.sleep(1)

    print("✅ Producer: 全メッセージ送信完了")


async def consumer():
    """Consumer: メッセージを受信・処理する側"""
    print("📥 Consumer: 開始")

    # 1. 既存メッセージを取得
    existing_messages = await client.lrange(MESSAGE_LIST, 0, -1)
    last_processed_index = len(existing_messages) - 1

    if existing_messages:
        print(f"   既存メッセージ {len(existing_messages)}件を取得:")
        for msg_json in existing_messages:
            msg = json.loads(msg_json)
            print(f"   → {msg['content']}")

    # 2. 新着通知を購読
    pubsub = client.pubsub()
    await pubsub.subscribe(NOTIFICATION_CHANNEL)

    print("👂 Consumer: 新着メッセージを待機中...")

    async for notification in pubsub.listen():
        if notification["type"] == "message" and notification["data"] == "new":
            # 3. 未処理の新着メッセージを取得
            new_messages = await client.lrange(
                MESSAGE_LIST, last_processed_index + 1, -1
            )
            last_processed_index += len(new_messages)

            for msg_json in new_messages:
                msg = json.loads(msg_json)
                print(f"🆕 新着 → {msg['content']}")


async def main():
    """メイン処理：ProducerとConsumerを並行実行"""
    # 前回のデータをクリア
    await client.delete(MESSAGE_LIST)

    # Producer と Consumer を同時に実行
    await asyncio.gather(producer(), consumer())


if __name__ == "__main__":
    print("=== Redis List + Pub/Sub 最小サンプル ===")
    print("リアルタイムメッセージングの基本実装\n")
    asyncio.run(main())

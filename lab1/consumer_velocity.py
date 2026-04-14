from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='velocity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# {user_id: [timestamp1, timestamp2, ...]}
user_timestamps = defaultdict(list)

WINDOW_SECONDS = 60
THRESHOLD = 3

print("Monitoring velocity anomalies (>3 transactions within 60s)...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    tx_time = datetime.fromisoformat(tx['timestamp'])

    # Thêm timestamp mới vào danh sách
    user_timestamps[user_id].append(tx_time)

    # Chỉ giữ lại các timestamps trong cửa sổ 60 giây gần nhất
    user_timestamps[user_id] = [
        t for t in user_timestamps[user_id]
        if (tx_time - t).total_seconds() <= WINDOW_SECONDS
    ]

    count = len(user_timestamps[user_id])

    if count > THRESHOLD:
        print(
            f"VELOCITY ALERT: {user_id} | "
            f"{count} transactions in last {WINDOW_SECONDS}s | "
            f"Latest: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}"
        )

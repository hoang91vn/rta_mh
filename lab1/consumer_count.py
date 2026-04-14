from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']

    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0.0) + tx['amount']
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n--- Summary after {msg_count} messages ---")
        print(f"{'Store':<12} {'Count':>6} {'Total Amount':>14} {'Avg Amount':>12}")
        print("-" * 48)
        for s in sorted(store_counts):
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count
            print(f"{s:<12} {count:>6} {total:>14.2f} {avg:>12.2f}")

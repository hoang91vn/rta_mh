from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {'count': 0, 'total': 0.0, 'min': float('inf'), 'max': float('-inf')})
msg_count = 0

for message in consumer:
    tx = message.value
    cat = tx['category']
    amount = tx['amount']

    stats[cat]['count'] += 1
    stats[cat]['total'] += amount
    stats[cat]['min'] = min(stats[cat]['min'], amount)
    stats[cat]['max'] = max(stats[cat]['max'], amount)
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n--- Category Stats after {msg_count} messages ---")
        print(f"{'Category':<12} {'Count':>6} {'Revenue':>12} {'Min':>10} {'Max':>10}")
        print("-" * 54)
        for cat in sorted(stats):
            s = stats[cat]
            print(f"{cat:<12} {s['count']:>6} {s['total']:>12.2f} {s['min']:>10.2f} {s['max']:>10.2f}")

#!/usr/bin/env python
from kafka import KafkaConsumer, KafkaProducer

import argparse


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--topic-name", help="topic to produce to")
    parser.add_argument("--server", help="kafka server to connect to")
    parser.add_argument("--total-msg", type=int, help="time to sleep")
    parser.add_argument("--consumer-group",  help="consume group to be used")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.server
    )
    for i in range(args.total_msg):
        producer.send(args.topic_name, b'msg %d' % i)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        args.topic_name,
        max_poll_records=1,
        api_version=(0, 11, 0),
        auto_offset_reset='earliest',
        bootstrap_servers=args.server,
        group_id=args.consumer_group,
        session_timeout_ms=60000,  # make this long to keep the group alive
        heartbeat_interval_ms=30000  # make this long to keep the group alive
    )

    # ensure we consume only 1 msg
    msg = None
    while not msg:
        msg = consumer.poll(timeout_ms=100, max_records=1)

    # will commit offset to 1
    consumer.commit()

    # voluntary dont close the client to keep the consumer group alive

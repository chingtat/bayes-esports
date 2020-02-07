import pika
import pandas as pd
import json

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Linear regressor worker.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--host', required=True, type=str, help='RabbitMQ host IP.')

    args = parser.parse_args()

    # ====
    connection = pika.BlockingConnection(pika.ConnectionParameters(args.host, connection_attempts=999, retry_delay=1))
    channel = connection.channel()

    #init
    channel.queue_declare(queue='queue_in_1')
    channel.queue_declare(queue='queue_in_2')

    channel.exchange_declare(exchange='features_queue',
                             exchange_type='direct')

    channel.queue_bind(exchange='features_queue',
                       queue='queue_in_1',
                           routing_key='route_to_queue_1')

    channel.queue_bind(exchange='features_queue',
                       queue='queue_in_2',
                           routing_key='route_to_queue_2')

    # ===================== #
    # fill queues with data #
    # ===================== #

    data = [
        ('route_to_queue_1', 'code_challenge_data1.csv'),
        ('route_to_queue_2', 'code_challenge_data2.csv'),
    ]

    for queue, filename in data:
        df = pd.read_csv(filename, index_col=0)

        for i, v in df.iterrows():
            msg = {
                'index': i,
                'features': list(v.values),
            }
            msg = json.dumps(msg, ensure_ascii=False)
            channel.basic_publish(
                exchange='features_queue', routing_key=queue, body=msg)

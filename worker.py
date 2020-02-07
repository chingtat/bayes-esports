import pika
import time
import threading as th
import configparser
import logging
import json
import numpy as np
from code_challenge_base_predictor import Predictor

class QueueWorker:
    def __init__(
            self,
            host='172.17.0.2',
            realtime_config_file='./config.ini',
            config_check_period=5,
            model_path='code_challenge_model.p',
            logfile='./worker.log'
    ):
        self.CONFIG_ALLOWED_QUEUES = ['queue_in_1', 'queue_in_2']

        self.CONFIG_RABBITMQ_HOST = host
        self.CONFIG_FILE_NAME = realtime_config_file
        self.CONFIG_CHECK_FOR_CONFIG_PERIOD = config_check_period
        self.CONFIG_CONSUMER_INACTIVITY_TIMEOUT = 0.1
        self.GLOBAL_CONFIG = {}

        self.GLOBAL_CONFIG['vars'] = self.get_config(
            self.CONFIG_FILE_NAME)  # maybe, once we would like to fully use of this config file

        self.__logfile = logfile
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # file
        fl = logging.FileHandler(self.__logfile, encoding='utf-8')
        fl.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fl.setFormatter(formatter)
        self.logger.addHandler(fl)

        # console
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        try:
            self.model = Predictor(model_path=model_path)
        except Exception as e:
            self.logger.exception(str(e))
            raise e

    def change_queue(self):
        while True:
            time.sleep(self.CONFIG_CHECK_FOR_CONFIG_PERIOD)

            # track change moment to execute code in it
            new_queue = self.get_current_queue(self.CONFIG_FILE_NAME)
            if not new_queue in self.CONFIG_ALLOWED_QUEUES:
                self.logger.warning(f'Unallowed queue name! '
                                    f'Change "current_queue" option in config file! '
                                    f'Allowed is: {self.CONFIG_ALLOWED_QUEUES} '
                                    f"Current is: {self.GLOBAL_CONFIG['curr_queue']}")
                continue

            if self.GLOBAL_CONFIG['curr_queue'] != new_queue:
                self.logger.info('Changing queue...')
                self.GLOBAL_CONFIG['curr_queue'] = new_queue

    def get_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config['DEFAULT']

    def get_current_queue(self, config_file):
        return self.get_config(config_file)['current_queue']

    def reset_consumer(self, channel, queue_name):
        return channel.consume(
            queue_name,
            auto_ack=False,
            inactivity_timeout=self.CONFIG_CONSUMER_INACTIVITY_TIMEOUT
        )

    def process_message(self, method, properties, body, channel, connection, queue_name):
        self.logger.debug(" [x] Received %r" % (body,))
        try:
            data = json.loads(body)
            features = np.array(data['features'])
            index = int(data['index'])
        except (IndexError, KeyError) as e:
            self.logger.warning("Message structure wrong: %r" % (body,))
            return
        except json.decoder.JSONDecodeError as e:
            self.logger.warning("JSON syntax error: %r" % (body,))
            return

        try:
            # predict one value
            predict = self.model.predict(features)[0]
        except Exception as e:
            self.logger.exception("Error running predictor: %r" % (str(e),))
            raise e

        self.logger.info("From queue %s item %d - Class 1 probability = %.3f"
                         % (queue_name, index, predict))
        connection.sleep(1)
        channel.basic_ack(method.delivery_tag)
        self.logger.debug(" [x] OK!")

    def run(self):
        try:
            # init
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                self.CONFIG_RABBITMQ_HOST, connection_attempts=999, retry_delay=1))
            self.GLOBAL_CONFIG['_curr_queue'] = None
            self.GLOBAL_CONFIG['curr_queue'] = self.get_current_queue(self.CONFIG_FILE_NAME)
            if self.GLOBAL_CONFIG['curr_queue'] not in self.CONFIG_ALLOWED_QUEUES:
                quit('Wrong queue in config! Change it!')

            channel = connection.channel()
            # declare queues
            for q in self.CONFIG_ALLOWED_QUEUES:
                channel.queue_declare(queue=q)

            queue_change_thread = th.Thread(target=self.change_queue, args=())
            queue_change_thread.start()
            consumer = None

            # loop
            while True:
                # if we received a new queue name or just started
                if self.GLOBAL_CONFIG['_curr_queue'] != self.GLOBAL_CONFIG['curr_queue']:
                    if consumer:
                        channel.cancel()
                    consumer = self.reset_consumer(channel, self.GLOBAL_CONFIG['curr_queue'])
                    self.GLOBAL_CONFIG['_curr_queue'] = self.GLOBAL_CONFIG['curr_queue']

                method, properties, body = next(consumer)
                if method is properties is body is None:
                    pass
                else:
                    self.process_message(
                        method,
                        properties,
                        body,
                        channel,
                        connection,
                        self.GLOBAL_CONFIG['curr_queue']
                    )

        except Exception as e:
            self.logger.exception(str(e))
            raise e


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Simple scikit-learn regression model worker.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--host', required=True, type=str, help='RabbitMQ host IP.')
    parser.add_argument('--realtime_config_file', type=str, default='./config.ini'
                        , help='Config which will be queried for master queue name change.')
    parser.add_argument('--config_check_period', type=float, default=5.0
                        , help='Period of querying config file for queue name change.')
    parser.add_argument('--model_path', type=str, default='./code_challenge_model.p'
                        , help='Model to serve on worker.')
    parser.add_argument('--log', type=str, default='./worker.log'
                        , help='File to store logs.')

    args = parser.parse_args()
    # ======

    app = QueueWorker(
        host=args.host,
        realtime_config_file=args.realtime_config_file,
        config_check_period=args.config_check_period,
        model_path=args.model_path,
        logfile=args.log
    )
    app.run()

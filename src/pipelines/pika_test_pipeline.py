import json

import pika

from helpers import PikaBlockingConnection


class PikaTestPipeline:
    def __init__(self):
        super(self.__class__, self).__init__()
        self.connection = None

    def open_spider(self, spider):
        self.connection = PikaBlockingConnection('test_queue_response')
        self.connection.rabbit_channel.queue_declare(queue=self.connection.queue_name, durable=True)
        self.connection.rabbit_channel.confirm_delivery()
        spider.logger.info(f'Init prisoner publisher with queue: {self.connection.queue_name}')

    def process_item(self, item, spider):
        self.connection.rabbit_channel.basic_publish(
            exchange='',
            routing_key=self.connection.queue_name,
            body=json.dumps(dict(item), default=str),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

        return item

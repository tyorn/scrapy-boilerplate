import json
import logging

import pika
from scrapy.utils.project import get_project_settings

from commands import BaseCommand
from helpers import PikaBlockingConnection


class TestPublisher(BaseCommand):
    _DEFAULT_PRODUCE_CHUNK_SIZE = 50

    def __init__(self):
        self.settings = get_project_settings()
        self.logger = logging.getLogger(f"scrapy.{self.__class__.__name__}")
        self.logger.setLevel(self.settings.get('LOG_LEVEL', 'INFO'))
        super(self.__class__, self).__init__(self.logger)

        self.queue_connection = None

    def init(self):
        self.queue_connection = PikaBlockingConnection('test_queue', self.settings)

    def run(self, args, opts):
        self.init()

        self.logger.info(f'Starting county_url_publisher with queue: {self.queue_connection.queue_name}')
        self.queue_connection.rabbit_channel.queue_declare(queue=self.queue_connection.queue_name, durable=True)
        self.queue_connection.rabbit_channel.confirm_delivery()

        queue_counter = self.queue_connection.rabbit_channel.queue_declare(
            queue=self.queue_connection.queue_name,
            passive=True).method.message_count

        if queue_counter > self._DEFAULT_PRODUCE_CHUNK_SIZE:
            return

        body_dict = {'url': 'https://myip.com'}
        self.queue_connection.rabbit_channel.basic_publish(
            exchange='',
            routing_key=self.queue_connection.queue_name,
            body=json.dumps(body_dict),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

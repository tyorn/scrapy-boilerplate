from datetime import datetime

import scrapy

from helpers import RMQObject


class OneToOneTestSpider(scrapy.Spider):
    name = 'one-one-test'

    custom_settings = {
        'EXTENSIONS': {
            'extensions.PikaBaseConsumer': 21,
        },
        'ITEM_PIPELINES': {
            'pipelines.pika_test_pipeline.PikaTestPipeline': 500,
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rmq_settings = dict({
            'queue': 'test_queue',
            'create_request_callback': self.__create_request
        })

    def __create_request(self, message: dict, rmq_object: RMQObject):
        return scrapy.Request(
            url=message['url'],
            callback=self.parse,
            errback=self.parse,
            meta={
                'rmq_object': rmq_object
            },
            dont_filter=True,
        )

    def parse(self, response):
        yield {
            'ip': response.css('#ip::text').get().strip(),
            'datetime': datetime.now(),
        }

        # this line will execute after item_error or item_dropped listeners callback complete
        response.meta['rmq_object'].ack()

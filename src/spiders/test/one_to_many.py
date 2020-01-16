from datetime import datetime

import scrapy

from helpers import RMQObject


class OneToManyTestSpider(scrapy.Spider):
    name = 'one-many-test'

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
        for i in range(2):
            yield {
                'id': i,
                'datetime': datetime.now(),
                'ip': response.css('#ip::text').get().strip()
            }

        retry = response.meta.get('retry', 0)
        if retry < 2:
            meta = response.meta
            meta.update({'retry': retry + 1})
            yield scrapy.Request(
                response.url,
                self.parse,
                dont_filter=True,
                meta=meta
            )
        else:
            response.meta['rmq_object'].ack()


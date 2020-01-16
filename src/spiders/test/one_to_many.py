from datetime import datetime

import scrapy


class OneToManyTestSpider(scrapy.Spider):
    name = 'one-many-test'

    custom_settings = {
        'EXTENSIONS': {
            'extensions.PikaBaseConsumer': 21,
        },
        'ITEM_PIPELINES': {
            'pipelines.pika_test_pipeline.PikaTestPipeline': 500,
        },
        'SPIDER_MIDDLEWARES': {
            'spidermiddlewares.AddRMQObjectToRequestMiddleware': 500,
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rmq_settings = dict({
            'queue': 'test_queue',
            'create_request_callback': self.__create_request
        })

    def __create_request(self, message: dict):
        return scrapy.Request(
            url=message['url'],
            callback=self.parse,
            errback=self.parse,
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
            yield scrapy.Request(
                response.url,
                self.parse,
                dont_filter=True,
                meta={'retry': retry + 1}
            )
        else:
            response.meta['rmq_object'].ack()

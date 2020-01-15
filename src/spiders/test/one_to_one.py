import scrapy


class OneToOneTestSpider(scrapy.Spider):
    custom_settings = {
        'EXTENSIONS': {
            'extensions.PikaBaseConsumer': 21,
        },
        'ITEM_PIPELINES': {
            'pipelines.pika_test_pipeline.PikaTestPipeline': 500,
        }
    }
    name = 'one-one-test'
    start_urls = []
    rmq_settings = dict()

    def __init__(self):
        self.rmq_settings['queue'] = 'test_queue'
        self.rmq_settings['create_request_callback'] = self.__create_request

    def __create_request(self, message: dict, rmq_object):
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
            'ip': response.css('#ip::text').get().strip()
        }


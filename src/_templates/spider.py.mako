## -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import scrapy


class ${class_name}(scrapy.Spider):
    name = "${spider_name}"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(${class_name}, cls).from_crawler(crawler, *args, **kwargs)
        return spider

    def __init__(self):
        scrapy.Spider.__init__(self)

    def start_requests(self):
        return []

    def parse(self, response):
        pass
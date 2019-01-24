# -*- coding: utf-8 -*-
import datetime
import hashlib

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http.request import Request

from crawler.items import CrawlerItem
from crawler.utils import get_first

class FazSpider(CrawlSpider):
    """Spider for 'Frankfurter Allgemeine Zeitung'"""
    name = 'faz'
    rotate_user_agent = True
    allowed_domains = ['www.faz.net']
    start_urls = ['http://www.faz.net']
    rules = (
        Rule(
            LinkExtractor(
                allow=(
                    'aktuell\/(politik|wirtschaft)\/.*\/$',
                    'aktuell\/(politik|wirtschaft)\/.*\/s\d+\.html',
                ),
            ),
            follow=True
        ),
        Rule(
            LinkExtractor(
                allow=('aktuell\/(politik|wirtschaft).+\.html'),
            ),
            callback='parse_page',
        ),
    )

    def parse_page(self, response):
        """Scrapes information from pages into items"""
        item = CrawlerItem()
        item['url'] = response.url.encode('utf-8')
        item['visited'] = datetime.datetime.now().isoformat().encode('utf-8')
        item['published'] = get_first(response.selector.xpath('//time/@datetime').extract())
        item['title'] = get_first(response.selector.xpath('//meta[@property="og:title"]/@content').extract())
        item['description'] = get_first(response.selector.xpath('//meta[@property="og:description"]/@content').extract()).strip()
        item['text'] = "".join([s.strip().encode('utf-8') for s in response.selector.xpath('//div[@class="atc-Text "]/p[@class="atc-TextParagraph"]/text()').extract()])
        item['author'] = [s.encode('utf-8') for s in response.selector.xpath('//span[@class="Autor"]/span[@class="caps last"]/a/span/text()').extract()]
        item['keywords'] = [s.encode('utf-8') for s in response.selector.xpath('//meta[@name="keywords"]/@content').extract()]
        item['resource']=self.name
        item['publication_id'] = str(hashlib.sha1((str(item['url']) + str(item['published']))))
        return item

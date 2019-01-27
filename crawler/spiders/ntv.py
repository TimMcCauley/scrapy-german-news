# -*- coding: utf-8 -*-
import datetime
import hashlib
import dateutil.parser as parser

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http.request import Request

from crawler.items import CrawlerItem
from crawler.utils import get_first

class NtvSpider(CrawlSpider):
    """Spider for 'n-tv'"""
    name = 'ntv'
    rotate_user_agent = True
    allowed_domains = ['www.n-tv.de']
    start_urls = ['http://www.n-tv.de','http://www.n-tv.de/thema/']
    rules = (
        Rule(
            LinkExtractor(
                allow=('(politik|wirtschaft|panorama|thema).*\/'),
                deny=('\.html')
            ),
            follow=True
        ),
        Rule(
            LinkExtractor(
                allow=(
                    '(politik|wirtschaft|panorama)\/.*article\d+\.html',
                ),
                deny=('mediathek|ticker')
            ),
            callback='parse_page',
        ),
    )

    def parse_page(self, response):
        """Scrapes information from pages into items"""
        item = CrawlerItem()
        item['url'] = response.url.encode('utf-8')
        item['visited'] = datetime.datetime.now().isoformat().encode('utf-8')
        item['published'] = parser.parse(get_first(response.selector.xpath('//meta[@name="date"]/@content').extract())).isoformat().encode('utf-8')
        item['title'] = get_first(response.selector.xpath('//meta[@property="og:title"]/@content').extract())
        item['description'] = get_first(response.selector.xpath('//meta[@name="description"]/@content').extract())
        item['text'] = "".join([s.strip().encode('utf-8') for s in response.selector.xpath('//p/text()').extract()])
        item['author'] = [s.encode('utf-8') for s in response.selector.xpath('//p[@class="author"]/em/text()').extract()]
        item['keywords'] = [s.encode('utf-8') for s in response.selector.xpath('//meta[@name="keywords"]/@content').extract()]
        item['resource'] = self.name
        item['publication_id'] = hashlib.sha1((str(item['url']) + str(item['published']))).hexdigest()
        return item

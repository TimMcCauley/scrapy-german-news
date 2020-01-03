# -*- coding: utf-8 -*-
import datetime
from pytz import timezone

import hashlib
import dateutil.parser as parser

from scrapy.exceptions import CloseSpider, DropItem, IgnoreRequest
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http.request import Request
#from scrapy.utils.project import get_project_settings

from crawler.items import CrawlerItem
from crawler.utils import get_first

class SZSpider(CrawlSpider):
    """Spider for 'Sueddeutsche Zeitung'"""
    name = 'sz'
    rotate_user_agent = True
    allowed_domains = ['www.sueddeutsche.de']
    start_urls = [
            'https://www.sueddeutsche.de/politik',
            'https://www.sueddeutsche.de/wirtschaft',
            'https://www.sueddeutsche.de/panorama'
            #'http://www.sueddeutsche.de/thema',
    ]
    rules = (
#        Rule(
#            LinkExtractor(
#                allow=('(politik|wirtschaft|panorama|thema)\/.+$',),
#                deny=('\.\d+','news')
#            ),
#            follow=True
#        ),
        Rule(
            LinkExtractor(
                allow=('(politik|wirtschaft|panorama)(\/\w+)*.*\.\d+$'),
            ),
            callback='parse_page',
        ),
    )

    def parse_page(self, response):
        """Scrapes information from pages into items"""
#        settings = get_project_settings()
        published = parser.parse(get_first(response.selector.xpath('//time/@datetime').extract()))
        published = published.replace(tzinfo=timezone('UTC'))
#        earliest = parser.parse(settings.get('EARLIEST_PUBLISHED'))
#        if published < earliest:
            #return []
#            raise DropItem('Dropping this article published on %s at %s which is before earliest published global setting %s' % (self.name, published.isoformat(), earliest.isoformat()))
            #raise CloseSpider('Article was published on %s at %s which is before earliest published global setting %s' % (self.name, published.isoformat(), earliest.isoformat()))
#        else:
    	item = CrawlerItem()
    	item['url'] = response.url.encode('utf-8')
    	item['visited'] = datetime.datetime.now().isoformat().encode('utf-8')
    	item['published'] = published.isoformat().encode('utf-8')
    	item['title'] = get_first(response.selector.xpath('//meta[@property="og:title"]/@content').extract())
    	item['description'] = get_first(response.selector.xpath('//meta[@name="description"]/@content').extract())
    	#item['text'] = "".join([s.strip().encode('utf-8') for s in response.selector.css('.article>.body>p').xpath('.//text()').extract()])
    	item['author'] = [s.encode('utf-8') for s in response.selector.css('.authorContainer').xpath('.//span/strong/span/text()').extract()]
    	item['keywords'] = [s.encode('utf-8') for s in response.selector.xpath('//meta[@name="news_keywords"]/@content').extract()]
    	item['resource'] = self.name
    	item['publication_id'] = hashlib.sha1((str(item['url']) + str(item['published']))).hexdigest()
    	return item

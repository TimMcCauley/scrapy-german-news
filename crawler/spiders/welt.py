# -*- coding: utf-8 -*-
import datetime
import hashlib
import dateutil.parser as parser
from pytz import timezone

from scrapy.exceptions import CloseSpider, DropItem, IgnoreRequest
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http.request import Request
#from scrapy.utils.project import get_project_settings

from crawler.items import CrawlerItem
from crawler.utils import get_first

class WeltSpider(CrawlSpider):
    """Spider for 'Die Welt'"""
    name = 'welt'
    rotate_user_agent = True
    allowed_domains = ['www.welt.de']
    start_urls = [
        'http://www.welt.de/politik',
        'http://www.welt.de/wirtschaft',
        #'http://www.welt.de/themen'
    ]
    rules = (
#        Rule(
#            LinkExtractor(
#                allow=('(politik|wirtschaft|vermischtes|themen)',),
#                deny=('\.html')
#            ),
#            follow=True
#        ),
        Rule(
            LinkExtractor(
                allow=('(politik|wirtschaft)(\/\w+)*\/article\d+\/.*\.html'),
            ),
            callback='parse_page',
        ),
    )

    def parse_page(self, response):
        """Scrapes information from pages into items"""
        #settings = get_project_settings()
        published = parser.parse(get_first(response.selector.xpath('//meta[@name="date"]/@content').extract()))
        published = published.replace(tzinfo=timezone('UTC'))
        #earliest = parser.parse(settings.get('EARLIEST_PUBLISHED'))
        #if published < earliest:
        #    raise DropItem('Dropping this article published on %s at %s which is before earliest published global setting %s' % (self.name, published.isoformat(), earliest.isoformat()))
            #raise CloseSpider('Article was published on %s at %s which is before earliest published global setting %s' % (self.name, published.isoformat(), earliest.isoformat()))
        #else:
    	item = CrawlerItem()
    	item['url'] = response.url.encode('utf-8')
    	item['visited'] = datetime.datetime.now().isoformat().encode('utf-8')
    	item['published'] = published.isoformat().encode('utf-8')
    	item['title'] = get_first(response.selector.xpath('//meta[@property="og:title"]/@content').extract())
    	item['description'] = get_first(response.selector.xpath('//meta[@name="description"]/@content').extract())
    	#item['text'] = "".join([s.strip().encode('utf-8') for s in response.selector.css('.artContent').xpath('.//text()').extract()])
    	item['author'] = [s.encode('utf-8') for s in response.selector.xpath('//meta[@name="author"]/@content').extract()]
    	item['keywords'] = [s.encode('utf-8') for s in response.selector.xpath('//meta[@name="keywords"]/@content').extract()]
    	item['resource'] = self.name
    	item['publication_id'] = hashlib.sha1((str(item['url']) + str(item['published']))).hexdigest()
    	return item

import scrapy
import os
import re
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule

from pystock_crawler import utils
from pystock_crawler.loaders import DividendItemLoader

class URLGenerator(object):

    def __init__(self, symbols, start_date='', end_date='', start=0, count=None):
        end = start + count if count is not None else None
        self.symbols = symbols[start:end]
        self.start_date = start_date
        self.end_date = end_date

    def __iter__(self):
        url = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&dateb=%s&datea=%s&owner=exclude&count=300&type=8-'
        #url = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=10-&dateb=%s&datea=%s&owner=exclude&count=300'
        for symbol in self.symbols:
            request= (url % (symbol, self.end_date, self.start_date))
            print request
            yield request

class DividendSpider(CrawlSpider):
    name = "dividend"
    allowed_domains = ['sec.gov']

    rules = (
        Rule(SgmlLinkExtractor(allow=('/Archives/edgar/data/[^\"]+\-index\.htm',))),
        Rule(SgmlLinkExtractor(allow=('/Archives/edgar/data/[^\"]+/[A-Za-z0-9_\-]+\.htm',)), callback='parse_8k'),
    )

    def __init__(self, **kwargs):
        super(DividendSpider, self).__init__(**kwargs)

        symbols_arg = kwargs.get('symbols')
        start_date = kwargs.get('startdate', '')
        end_date = kwargs.get('enddate', '')
        limit_arg = kwargs.get('limit', '')

        utils.check_date_arg(start_date, 'startdate')
        utils.check_date_arg(end_date, 'enddate')
        start, count = utils.parse_limit_arg(limit_arg)

        if symbols_arg:
            if os.path.exists(symbols_arg):
                # get symbols from a text file
                symbols = utils.load_symbols(symbols_arg)
            else:
                # inline symbols in command
                symbols = symbols_arg.split(',')
            self.start_urls = URLGenerator(symbols, start_date, end_date, start, count)
        else:
            self.start_urls = []

    def parse_8k(self, response):
        
        loader = DividendItemLoader(response=response)
        item = loader.load_item()
        if 'doc_type' in item:
            doc_type = item['doc_type']
            if doc_type in ['8-K']:
                if 'pay_date' in item and item['pay_date'] != '':
                    return item
        return None

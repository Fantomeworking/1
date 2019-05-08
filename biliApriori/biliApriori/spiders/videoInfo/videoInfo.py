import re
import time
import scrapy
from scrapy import Request
from biliApriori.items import BiliaprioriItem
class videoInfo(scrapy.Spider):
    name="Info"
    allowed_domains = ["bilibili.com"]

    start_urls = ["https://www.bilibili.com/ranking/all/1/1/3"]

    def parse(self,response):
        path1='//*[@id="app"]/div[2]/div/div[1]/div[2]/div[3]/ul/li[1]'
        path1_2="//div[2]/div[2]/a/@href"
        # 爬取视频对应的Av号
        info=response.xpath(path1).xpath(path1_2).extract()
        for eachInfo in info:
            self.newUrl='https:'+eachInfo
            yield Request(self.newUrl,
                          callback = self.parse_detail,
                          headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36'})
    def parse_detail(self, response):
        path2 = '//*[@id="v_tag"]/ul/li/a'
        tag=response.xpath(path2).extract()
        item=BiliaprioriItem()
        tag_Str=re.findall(">(.+?)</a>", str(tag))
        item["tagInfo"]=",".join(tag_Str)
        yield item


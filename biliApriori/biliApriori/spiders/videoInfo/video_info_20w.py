import copy
import json
import re
import pymysql
import scrapy
import datetime
from scrapy import Request
from biliApriori.items import BiliaprioriItem
import logging
#headers='Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1'
#45750000ge 个视频
#50461141
#{aid,view}

class videoInfo(scrapy.Spider):
    name="Info_big"
    allowed_domains = ["bilibili.com"]
    def __init__(self):
        # 获取上次停止的AV号,hdfs方式
        # self.client = Client("http://fantome:50070")
        #
        # with self.client.read('/bili_3-7day/last_aid.txt') as reader:
        #     start_Num_str =reader.read().decode()
        #     self.start_Num =int(start_Num_str)+1
        # 获取上次停止的AV号，sql方式
        conn=pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='6194clllr',
            database='bili',
            charset="utf8")#获得链接
        cursor=conn.cursor()#获得指针
        cursor.execute("select * from lastAid")
        self.start_Num = cursor.fetchone()[0]+1
        cursor.execute("select count(*) from lastAid")
        self.fit_Nums = cursor.fetchone()[0]
        print("startAid: ",self.start_Num)
        cursor.close()
        conn.close()
        logging.warning(("this is not a warning and scrapy start to work","\nGet startNum：",self.start_Num,"Get fitNums：",self.fit_Nums))
        self.stop_Num=100000000
        self.spider_stop=0#记录有多少视频连续出现在3天内

    def start_requests(self):
        # 爬取视频对应的Av号
        for i in range(self.start_Num,self.stop_Num):
            aids=i
            #print("当前AV号：av",i,"\n扫描视频量：",i-self.start_Num)
            self.url='http://api.bilibili.com/archive_stat/stat?aid='+str(aids)
            yield Request(url = self.url,
                          callback=self.video_fit
                        )
    def video_fit(self,response):
        data = json.loads(response.text)
        allow=data["message"]
        if allow=='0':
            info=data["data"]#信息
            views=info["view"]#播放量
            #like：点赞，dislike：踩，favorite：收藏，danmaku：弹幕量，coin:硬币，share:分享,
            aid=info["aid"]#av号
            #筛选av号
            # if self.fit_Nums>10000:#视频数量大于1万退出程序
            #     self.crawler.engine.close_spider(self, '1万数据到手，停止爬虫!')
            if views>10000:#播放量大于1万
                self.fit_Nums+=1
                print("视频", self.fit_Nums, "\nAV号：av", aid, "\n播放量：", views)
                #当前视频aid,问题在parse_detail没开始之前，下一个aid进来，这个aid可能会被顶掉

                self.newUrl='https://www.bilibili.com/video/av'+str(aid)
                yield Request(self.newUrl,
                              callback = lambda response :self.parse_detail(response,aid,views))#多位传参
    def parse_detail(self,response,aid,views):
        path_time= '// *[ @ id = "viewbox_report"] / div[1] / span[2]'#视频获取时间
        time = response.xpath(path_time).extract()[0]
        videoTime=re.findall(">(.+?)</span>", time)#视频发表时间

        xtime,vtime,vUTCtimestamp=self.XTime(videoTime)
        # 视频连续在3天内超过10个时退出
        self.stopAndSaveLastAid(xtime)

        path_tag = '//*[@id="v_tag"]/ul/li/a'
        tag=response.xpath(path_tag).extract()
        item=BiliaprioriItem()
        tag_Str=re.findall(">(.+?)</a>", str(tag))
        item["aid"] = aid
        item["views"] =views
        item["tagInfo"]=",".join(tag_Str)
        item["tagTime"]=str(vUTCtimestamp)
        print("Get Time","  发布日期：",vtime,"距离现在",xtime,"天")
        yield item
    def XTime(self,videoTime):
        #获取utc时间，转化成时间戳-本地时间，转化成时间戳
        nowUTCtime_date = datetime.datetime.utcnow()#现在utc时间xx:xx:xx
        now_UTCtimestamp = datetime.datetime.timestamp(nowUTCtime_date)#转化为utc时间戳
        vtime=datetime.datetime.strptime(videoTime[0], "%Y-%m-%d %H:%M:%S")  #视频当地时间
        vUTCtimestamp = datetime.datetime.timestamp(vtime)-28800#视频utc时间戳
        xtime=(now_UTCtimestamp-vUTCtimestamp)/86400
        return xtime,vtime,vUTCtimestamp
    def stopAndSaveLastAid(self,xtime):
        if xtime < 3:
            self.spider_stop += 1
        if self.spider_stop > 0 and xtime > 3:
            self.spider_stop = 0
        if self.spider_stop > 10:
            # 停止时，保存当前的AV号,使用hdfs的方式->改为使用sql
            #self.client.write('/bili_3-7day/last_aid.txt', str(last_aid) + "\n", overwrite=True, append=False)
            self.crawler.engine.close_spider(self, '时间距离现在为3天，停下爬虫!')







    # # 使用hdfs的方式读写，速率太低，放弃
    # def delete_hdfs(self):
    #     with self.client.read('/bili_3-7day/tagInfo.txt') as reader:
    #         tagInfo = reader.read().decode()
    #     with self.client.read('/bili_3-7day/tag_time.txt') as reader:
    #         tag_time = reader.read().decode()
    #     xtagInfo = re.findall("(.+?)\\n", tagInfo)
    #     xtag_time = re.findall("(.+?)\\n", tag_time)
    #     if len(xtagInfo) != len(xtag_time):
    #         self.crawler.engine.close_spider(self, 'Tag与time不同步，停止爬虫!')
    #     print('Tag数据量：', len(xtagInfo))
    #     self.fit_Nums += len(xtagInfo)
    #     nowUTCtime_date = datetime.datetime.utcnow()
    #     now_UTCtimestamp = datetime.datetime.timestamp(nowUTCtime_date)
    #     x = 0
    #     for s, timeUTCStamp in enumerate(xtag_time):
    #         if now_UTCtimestamp - float(timeUTCStamp) <= 604800:  # 7天内
    #             if x == 0:
    #                 overwrite, append = True, False
    #             else:
    #                 overwrite, append = False, True
    #             x = 1
    #             self.client.write('/bili_3-7day/tagInfo.txt', xtagInfo[s - 1] + "\n", overwrite=overwrite,
    #                               append=append, encoding="utf-8")
    #             self.client.write('/bili_3-7day/tag_time.txt', xtag_time[s - 1] + "\n", overwrite=overwrite,
    #                               append=append, encoding="utf-8")

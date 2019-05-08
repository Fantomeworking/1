# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from DBUtils.PersistentDB import PersistentDB
from DBUtils.PooledDB import PooledDB
from hdfs import Client


class BiliaprioriPipeline(object):
    def __init__(self):
        self.client=Client("http://fantome:50070")

    def process_item(self, item, spider):
        print("Get Tag","  tag:",item['tagInfo'])

        #持续写入
        self.client.write('/bili_3-7day/tagInfo.txt',item['tagInfo']+"\n",overwrite=False,append=True,encoding="utf-8")
        # with open("tagInfo.txt", 'a') as fp:
        #     fp.write(item['tagInfo']+"\n")
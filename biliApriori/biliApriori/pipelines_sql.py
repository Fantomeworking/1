# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import logging
import pymysql
from DBUtils.PooledDB import PooledDB



class BiliaprioriPipeline(object):
    tag = []
    tag_weight = []
    def open_spider(self, spider):
        self.pool = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=0, # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            host='127.0.0.1',
            port=3306,
            user='root',
            password='6194clllr',
            database='bili',
            charset='utf8'
        )
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        try:
            # 爬虫开始时，删除超过7天的数据
            self.cursor.execute("select count(*) from tag")
            all = self.cursor.fetchone()[0]
            nowUTCtime_date = datetime.datetime.utcnow()
            now_UTCtimestamp = datetime.datetime.timestamp(nowUTCtime_date)
            outtime = now_UTCtimestamp - 2592000
            SQL = "delete from tag where time<=" + str(int(outtime))
            SQL_weight = "delete from tag_weight where time<=" + str(int(outtime))
            self.cursor.execute(SQL)
            self.cursor.execute(SQL_weight)
            self.cursor.execute("select count(*) from tag")
            left=self.cursor.fetchone()[0]
            self.conn.commit()
            logging.warning(("过时数据删除成功","剩余数据:",left,"删除条数：",all-left))
            print("过时数据删除成功","剩余数据:",left,"删除条数：",all-left)
        except:
            logging.warning(("过时数据删除失败"))
            print("过时数据删除失败")
            self.conn.rollback()

        # self.cursor.execute("truncate table tag")
        # self.conn.commit()

    # 批量插入
    def bulk_insert_to_mysql(self, bulkdata,bulkdata_weight):
        try:
            sql = "INSERT INTO tag (aid,views,tag,time) VALUES (%s,%s, %s, %s)"
            sql_weight = "INSERT INTO tag_weight (aid,views,tag,time) VALUES (%s,%s, %s, %s)"
            self.cursor.executemany(sql, bulkdata)
            self.cursor.executemany(sql_weight, bulkdata_weight)
            self.conn.commit()
            print ("上传成功 , tag.batch:",len(bulkdata),"tag_weight.batch:",len(bulkdata_weight))
            logging.warning(("上传数据成功, batch:",len(bulkdata),"tag_weight.batch:",len(bulkdata_weight)))
        except:
            self.conn.rollback()
            logging.warning(["上传数据失败"])
    def process_item(self, item, spider):
        print("Get Tag", "   tag:", item['tagInfo'],"\naid:", item['aid'],"views:", item['views'])
        self.tag.append((item["aid"], item["views"], item['tagInfo'], item['tagTime']))
        #加权
        for i in range(item['views']//10000):
            self.tag_weight.append((item["aid"],item["views"],item['tagInfo'], item['tagTime']))
        self.lastAid=item["aid"]
        if len(self.tag_weight) > 100:#小批量上传
            self.bulk_insert_to_mysql(self.tag,self.tag_weight)
            # 清空缓冲区
            del self.tag[:]
            del self.tag_weight[:]
        return item

    # spider结束
    def close_spider(self, spider):
        print("closing spider,last Aid", self.lastAid)
        logging.warning(("当前最后一批数据上传"))
        self.cursor.execute("truncate table lastAid")#先清空lastAid，在写入最新的lastAid
        save_LastAid_sql="insert into lastAid values("+str(self.lastAid)+")"
        self.cursor.execute(save_LastAid_sql)
        self.conn.commit()

        self.bulk_insert_to_mysql(self.tag,self.tag_weight)
        self.cursor.close()
        self.conn.close()
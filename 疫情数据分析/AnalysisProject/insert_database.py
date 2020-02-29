# 向数据库中插入数据
from crawl_data import CollectData
import pandas as pd
import pymysql

class InsertDatabase:
    def __init__(self):
        self.callable = CollectData()

    def insert(self):
        data = self.callable.filtration_data()
        coon = pymysql.connect(host='127.0.0.1', user='root', password="123456", database="epidemic_data",
                               charset="utf8")
        # number = int(pd.read_sql("select cycle from data_all order by id DESC limit 1", coon)["cycle"].to_list()[0]) + 1
        # print("正在向阿里云服务器插入数据: ", number)
        number = 1
        cursor = coon.cursor()  # 创建事务
        sql = "insert into data_all(cityName, confirmedCount, curedCount, deadCount, province_name, " \
              "date_info, detail_time, cycle) values(%s, %s, %s, %s, %s, %s, %s, %s)"

        print("正在插入数据...")
        for cityName, confirmedCount, curedCount, deadCount, province_name, date_info, detail_time in zip(data["城市"],
                                                                                                          data["确诊"],
                                                                                                          data["治愈"],
                                                                                                          data["死亡"],
                                                                                                          data["省份"],
                                                                                                          data["日期"],
                                                                                                          data["时间"]):
            cursor.execute(sql, (
                cityName, confirmedCount, curedCount, deadCount, province_name, date_info, detail_time, number))
            coon.commit()
        print("数据插入完成...")
        cursor.close()
        coon.close()

my_insert = InsertDatabase()
my_insert.insert()
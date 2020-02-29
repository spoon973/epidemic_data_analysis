# 编写爬虫程序收集数据
import datetime
import pandas as pd
import re
import redis
import requests

class CollectData:
    def __init__(self):
        self.url = 'https://3g.dxy.cn/newh5/view/pneumonia?scene=2&clicktime=1579582238&enterid=1579582238&from=timeline&isappinstalled=0'
        self.all_data = list()
        self.red = redis.Redis(host='localhost', port=6379, db=1)
        self.host_ip = "127.0.0.1"
        self.host_user = "root"

    def request_page(self):
        """
        请求页面数据
        """
        res = requests.get(self.url)
        res.encoding = 'utf - 8'
        pat0 = re.compile('window.getAreaStat = ([\s\S]*?)</script>')
        data_list = pat0.findall(res.text)
        data = data_list[0].replace('}catch(e){}', '')
        data = eval(data)
        return data

    def deep_spider(self, data, province_name):
        """
        深度提取出标签里详细的数据
        :param data:
        :param province_name:
        :return:
        """
        for temp_data in data:
            self.all_data.append([temp_data["cityName"], temp_data["confirmedCount"], temp_data["curedCount"],
                                  temp_data["deadCount"], province_name, datetime.date.today(),
                                  datetime.datetime.now().strftime('%H:%M:%S')])

    def filtration_data(self):
        """
        过滤数据
        """
        temp_data = self.request_page()
        province_short_names, confirmed_counts, cured_counts, dead_counts = list(), list(), list(), list()
        for i in temp_data:
            province_short_names.append(i['provinceShortName'])  # 省份
            confirmed_counts.append(i['confirmedCount'])  # 确诊
            cured_counts.append(i['curedCount'])  # 治愈
            dead_counts.append(i['deadCount'])  # 死亡
            # 深度解析数据添加到实例属性中
            self.deep_spider(i['cities'], i["provinceShortName"])
        data_all = pd.DataFrame(self.all_data, columns=["城市", "确诊", "治愈", "死亡", "省份", "日期", "时间"])
        df = pd.DataFrame()
        df['省份'] = province_short_names
        df['确诊'] = confirmed_counts
        df['治愈'] = cured_counts
        df['死亡'] = dead_counts
        # print(df)
        return data_all




#
# def main():
#     coll_data = CollectData()
#     coll_data.filtration_data()
#
#
# if __name__ == "__main__":
#     main()
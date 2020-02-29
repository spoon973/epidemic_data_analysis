# 对数据进行分析
import redis
import pandas as pd
import pymysql

class DataAnalysis:
    '''
    数据分析返回结果
    '''
    def __init__(self):
        self.red = redis.Redis(host='localhost', port=6379, db=1)

    def my_print(self):
        print(self.red.flushdb())
        coon = pymysql.connect(host="127.0.0.1", user="root", password="123456", database="epidemic_data",
                               charset="utf8")
        print("连接数据库成功")
        data_all = pd.read_sql("select * from data_all", coon)
        # 查询最新一次数据的索引
        number = pd.read_sql("select cycle from data_all order by id "
                             "DESC limit 1", coon)["cycle"].to_list()[0]
        # 查询最新一次的数据
        data1 = data_all[data_all["cycle"] == number]
        # 查询距离上一次12小时前的数据
        data2 = data_all[data_all["cycle"] == number - 1]

        # 1.提取出目前有多少感染的人
        confirmed_all = data1["confirmedCount"].sum()  # 总感染人数 redis.set.confirmed_all
        cured_all = data1["confirmedCount"].sum() - data2["confirmedCount"].sum()  # 今日新增病例数 redis.set.cured_all
        # 向redis数据库中存储 总感染人数 和 今日新增病例数
        self.red.set("confirmed_all", int(confirmed_all))
        self.red.set("cured_all", int(cured_all))
        # 查询出感染人数排名前七的城市
        serious_all = data1[["cityName", "confirmedCount"]].sort_values(by="confirmedCount", ascending=False)[0:7]
        # 将排名前7的城市名存放列表当中
        serious_all_index = serious_all["cityName"].tolist()  # redis.serious_all_index
        # 将排名前7的城市感染数存放列表当中
        serious_all_value = serious_all["confirmedCount"].tolist()  # redis.serious_all_value
        # Rpush 命令用于将一个或多个值插入到列表的尾部(最右边)。
        # 将感染人数前七的城市和人数用尾插法依次插入redis数据库中
        [self.red.rpush("serious_index", x) for x in serious_all_index]
        [self.red.rpush("serious_value", x) for x in serious_all_value]
        # 计算增长人数排名前九的城市
        add_nums = (data1.groupby("province_name")["confirmedCount"].sum() - data2.groupby("province_name")[
            "confirmedCount"].
                    sum()).sort_values(ascending=False)[1:10]  # 因为湖北增长比较多其他数据就显的比较小所以没有计算湖北
        # 将增长人数排名前九的城市用尾插法依次插入redis数据库中
        [self.red.rpush("add_nums_index", x) for x in add_nums.index.tolist()]
        [self.red.rpush("add_nums_value", x) for x in add_nums.values.tolist()]
        # 因全国确诊比例以湖北为重，因此用湖北为样标，计算确诊比例
        confirmed_num = data1.groupby("province_name")["confirmedCount"].sum()  # 确诊比例
        confirmed_num = [confirmed_num["湖北"], (confirmed_num.sum() - confirmed_num["湖北"])]
        [self.red.rpush("confirmed_num", int(x)) for x in confirmed_num]
        # 因全国死亡比例以湖北为重，因此用湖北为样标，计算死亡比例
        deed_num = data1.groupby("province_name")["deadCount"].sum()  # 死亡比例
        deed_num = [deed_num["湖北"], (deed_num.sum() - deed_num["湖北"])]
        [self.red.rpush("deed_num", int(x)) for x in deed_num]
        # 因全国增长比例以湖北为重，因此用湖北为样标，计算增长比例
        add_num1 = data1.groupby("province_name")["confirmedCount"].sum()  # 增长比例
        add_num2 = data2.groupby("province_name")["confirmedCount"].sum()
        add_num = (add_num1 - add_num2).sort_values(ascending=False)
        '''
        # 因数据不全，明天第二次爬取时开启
        # [self.red.rpush("add_nums", int(x)) for x in [add_num["湖北"], add_num.sum() - add_num["湖北"]]]'''

        # 获取每天上午爬去数据的 cycle 索引
        temp_list = [x for x in range(number + 1) if x % 2 != 0]
        '''
        数据不全，暂不开启
        # 提取出前12次数据每天上午确诊病例的人数总和
        confirmed_list = [[data_all[data_all["cycle"] == num]["date_info"].unique()[0],
                           data_all[data_all["cycle"] == num]["confirmedCount"].sum()] for num in
                          (temp_list if len(temp_list) <= 12 else temp_list[len(temp_list) - 12:])]
        # 提取出前12次数据每天上午治愈人数总和
        cured_list = [[data_all[data_all["cycle"] == num]["date_info"].unique()[0],
                       data_all[data_all["cycle"] == num]["curedCount"].sum()] for num in
                      (temp_list if len(temp_list) <= 12 else temp_list[len(temp_list) - 12:])]
        # 将数据转换为pandas.DataFrame
        confirmed_temp = pd.DataFrame(confirmed_list, columns=["date", "num"])
        cured_temp = pd.DataFrame(cured_list, columns=["date", "num"])
        # 相隔天数的数据相减得到数据列表【日期，确诊病例每日增长数，治愈病例每日增长数量】
        line_data = [confirmed_temp["date"].apply(lambda x: x.split("-")[-1]).tolist(),
                     (confirmed_temp["num"] - confirmed_temp["num"].shift(1)).tolist(),
                     (cured_temp["num"] - cured_temp["num"].shift(1)).tolist()]
        [self.red.rpush("line_data_date", x) for x in line_data[0]]
        [self.red.rpush("line_data_value1", x) for x in line_data[1]]
        [self.red.rpush("line_data_value2", x) for x in line_data[2]]
        '''
        # 康复人数与死亡人数
        bar_num = [data1["curedCount"].sum(), data1["deadCount"].sum()]
        [self.red.rpush("bar_num", int(x)) for x in bar_num]
        print(bar_num)
        # 地图数据
        map_df = data1[["cityName", "confirmedCount"]].sort_values(by="confirmedCount", ascending=False)
        # print(map_df["cityName"].tolist())
        # print((map_df["confirmedCount"] * 0.01).tolist())
        [self.red.rpush("map_index", x) for x in map_df["cityName"].tolist()]
        [self.red.rpush("map_value", x) for x in (map_df["confirmedCount"]).tolist()]






if  __name__ == "__main__":
    data_analysis = DataAnalysis()
    data_analysis.my_print()
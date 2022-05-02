# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import re
import time

import pymongo
from difflib import SequenceMatcher
from weibo_trajectory_incremental_crawler.items import WeiboItem, UserItem, UserRelationItem, TrajectoryItem
from elasticsearch import Elasticsearch


class TimePipeline():
    def process_item(self, item, spider):
        if isinstance(item, UserItem) or isinstance(item, WeiboItem):
            now = time.strftime('%Y-%m-%d %H:%M', time.localtime())
            item['crawled_at'] = now
        return item


class MongoPipeline(object):
    def __init__(self, local_mongo_host, local_mongo_port, mongo_db):
        self.local_mongo_host = local_mongo_host
        self.local_mongo_port = local_mongo_port
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):

        return cls(
            local_mongo_host=crawler.settings.get('LOCAL_MONGO_HOST'),
            local_mongo_port=crawler.settings.get('LOCAL_MONGO_PORT'),
            mongo_db=crawler.settings.get('DB_NAME')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.local_mongo_host, self.local_mongo_port)
        # 数据库名
        self.db = self.client[self.mongo_db]
        # 以Item中collection命名 的集合（数据库表）  添加index
        self.db[UserItem.collection].create_index([('id', pymongo.ASCENDING)])
        self.db[WeiboItem.collection].create_index([('id', pymongo.ASCENDING)])
        self.db[UserRelationItem.collection].create_index([('id', pymongo.ASCENDING)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if isinstance(item, UserItem) or isinstance(item, WeiboItem):
            self.db[item.collection].update({'id': item.get('id')},
                                            {'$set': item},
                                            True)
        if isinstance(item, UserRelationItem):
            self.db[item.collection].update(
                {'id': item.get('id')},
                {'$addToSet':
                    {
                        'follows': {'$each': item['follows']},
                        'fans': {'$each': item['fans']}
                    }
                },
                True)
        return item


class WeiboPipeline():
    def parse_time(self, date):
        if re.match('刚刚', date):
            date = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
        if re.match('\d+分钟前', date):
            minute = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time() - float(minute) * 60))
        if re.match('\d+小时前', date):
            hour = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time() - float(hour) * 60 * 60))
        if re.match('昨天.*', date):
            date = re.match('昨天(.*)', date).group(1).strip()
            date = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + ' ' + date
        if re.match('\d{2}-\d{2}', date):
            date = time.strftime('%Y-', time.localtime()) + date + ' 00:00'
        return date

    def process_item(self, item, spider):
        if isinstance(item, WeiboItem):
            if item.get('created_at'):
                item['created_at'] = item['created_at'].strip()
                item['created_at'] = self.parse_time(item.get('created_at'))
            if item.get('pictures'):
                item['pictures'] = [pic.get('url') for pic in item.get('pictures')]

        return item


class TrajectoryPipline():

    def __init__(self):
        self.dic = {}
        self.traj_list = []
        self.PATTERN = r'([\u4e00-\u9fa5]{1,15}(村|街|街道|小区|市场|中心|路|城|广场|行|大道|店|公司|山|立交|超市|站|局|栋|单元|场|区|巷|公园|馆|部|边|仓)){1}'
        self.PATTERN1 = r'((0?[1-9]|1[0-2])月(0?[1-9]|[1-2][0-9]|3[0-1])日)'
        self.PATTERN2 = r'场所通报如下'
        self.PATTERN3 = r'防控指挥部通报'
        self.PATTERN4 = r'((20|21|22|23|[0-1]\d|\d):([0-5]\d|\d))'
        self.PATTERN5 = r'(——|-|至|到|左右至|左右到|左右-|左右——|—|左右—)'
        self.PATTERN6 = r'\d\d\d路公交'
        self.PATTERN7 = r'(居住于|现居于|居住在|住所为|住所是)'
        self.PATTERN8 = r'(，|,|。|；|;|.)'
        self.date_period_list = []
        self.pattern = re.compile(self.PATTERN)
        self.pattern1 = re.compile(self.PATTERN1)

    def trans_format(self, time_string, from_format, to_format='%Y-%m-%d %H:%M:%S'):
        time_struct = time.strptime(time_string, from_format)
        times = time.strftime(to_format, time_struct)
        return times

    def handle_time(self, act_year, act_str_pre, date_str_pre, pattern_time_period, pattern_time):
        time_match = re.search(pattern=pattern_time_period, string=act_str_pre)
        if time_match is not None and time_match.lastindex is not None:
            time_str = time_match.group(0)
            act_str_pre = act_str_pre.split(time_str)[1]
            begin_time_pre = time_match.group(1)
            end_time_pre = time_match.group(5)
            traj_item_pre = {time.strptime(act_year + date_str_pre + begin_time_pre, "%Y年%m月%d日%H:%M"): act_str_pre,
                             time.strptime(act_year + date_str_pre + end_time_pre, "%Y年%m月%d日%H:%M"): act_str_pre}
            return traj_item_pre
        else:
            time_match = re.search(pattern=pattern_time, string=act_str_pre)
            if time_match is not None and time_match.lastindex is not None:
                time_str = time_match.group(0)
                act_str_pre = act_str_pre.split(time_str)[1]
                traj_item_pre = {
                    time.strptime(act_year + date_str_pre + time_str, "%Y年%m月%d日%H:%M"): act_str_pre}
                return traj_item_pre
            else:
                traj_item_pre = {
                    time.strptime(act_year + date_str_pre + "00:00", "%Y年%m月%d日%H:%M"): act_str_pre}
                return traj_item_pre

    def process_each_phrase(self, act_year, list_use, dic, traj_list, address_list, extra_str_list):
        for l in list_use:
            l = l.replace('\n', '').replace('\r', '').replace(" ", "").replace("：", ":")
            extra_str_list.append(l)
            msg = re.search(pattern=self.pattern, string=l)
            if msg is not None and msg.lastindex is not None:
                if msg.lastindex >= 1:
                    # 仅当处理子串中包含地名时可进一步处理，其余的当做额外信息存储
                    # 分析子串中的日期时间和病例居住地
                    date = re.search(pattern=self.pattern1, string=l)
                    address = re.search(pattern=re.compile(self.PATTERN7), string=l)
                    if address is not None:
                        if address.lastindex is not None:
                            # 居住地非空时忽略后续时间处理
                            address_split_str = l.split(address.group(0))[1].split('，')
                            address_str = address_split_str[0]
                            address_list.append(address_str)
                    else:
                        # 循环处理当前子串的时空数据，每次对一条有效时间做分析
                        date_str_pre = "1月1日"
                        rest_str_pre = l
                        pattern_time_period = re.compile(self.PATTERN4 + self.PATTERN5 + self.PATTERN4)
                        pattern_time = re.compile(self.PATTERN4)

                        if date is not None and date.lastindex is not None:
                            date_str_pre = date.group(0)
                            rest_str_pre = l.split(date_str_pre)[1]
                            date = re.search(pattern=self.pattern1, string=rest_str_pre)

                            while date is not None and date.lastindex is not None:
                                # 切去日期
                                date_str_cur = date.group(0)
                                act_str_pre = rest_str_pre.split(date_str_cur, 1)[0]
                                act_str_cur = rest_str_pre.split(date_str_cur, 1)[1]

                                traj_item_pre = self.handle_time(act_year, act_str_pre, date_str_pre,
                                                                 pattern_time_period, pattern_time)
                                for k in traj_item_pre:
                                    if len(traj_item_pre[k]) < 2:
                                        self.date_period_list.append(k)
                                    else:
                                        for dateK in self.date_period_list:
                                            dic[dateK] = traj_item_pre[k]
                                            traj_list.append(
                                                (time.strftime("%Y-%m-%d %H:%M:%S", dateK) + '@' + traj_item_pre[k]))
                                        self.date_period_list.clear()
                                date = re.search(pattern=self.pattern1, string=act_str_cur)
                                date_str_pre = date_str_cur
                                rest_str_pre = act_str_cur
                                dic.update(traj_item_pre.items())
                                for k in traj_item_pre:
                                    if len(traj_item_pre[k]) >= 2:
                                        traj_list.append((time.strftime("%Y-%m-%d %H:%M:%S", k) + '@' + traj_item_pre[k]))

                            traj_item_pre = self.handle_time('2022年', rest_str_pre, date_str_pre, pattern_time_period,
                                                             pattern_time)
                            for k in traj_item_pre:
                                if len(traj_item_pre[k]) >= 2:
                                    for dateK in self.date_period_list:
                                        dic[dateK] = traj_item_pre[k]
                                        traj_list.append(
                                            (time.strftime("%Y-%m-%d %H:%M:%S", dateK) + '@' + traj_item_pre[k]))
                                    self.date_period_list.clear()
                            dic.update(traj_item_pre.items())
                            for k in traj_item_pre:
                                if len(traj_item_pre[k]) >= 2:
                                    traj_list.append((time.strftime("%Y-%m-%d %H:%M:%S", k) + '@' + traj_item_pre[k]))

    def process_item(self, item, spider):
        trajectory_item = TrajectoryItem()
        dic = {}
        traj_list = []
        address = []
        extra_info = []
        if isinstance(item, WeiboItem):
            text = item["text"]
            created_time = item['created_at']
            item['created_at'] = self.trans_format(created_time, '%a %b %d %H:%M:%S +0800 %Y', '%Y-%m-%d %H:%M:%S')
            act_year = time.strptime(item['created_at'], '%Y-%m-%d %H:%M:%S').tm_year.__str__() + "年"

            split_str_test = text.split(self.PATTERN2, 1)
            if len(split_str_test) == 1:
                split_str_test = text.split(self.PATTERN3, 1)
                if len(split_str_test) == 1:
                    split_str_test = ["", text]

            list_use = split_str_test[1].split("@@@###")
            self.process_each_phrase(act_year, list_use, dic, traj_list, address, extra_info)
            trajectory_item["address"] = address
            trajectory_item["trajectory"] = dic
            trajectory_item["trajectory_list"] = traj_list
            trajectory_item["extra_info"] = extra_info

        return trajectory_item


class ElasticSearchPipeline(object):

    def __init__(self, local_es_hosts):
        self.local_es_hosts = local_es_hosts
        self.index = 'risk_trajectory'

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            local_es_hosts=crawler.settings.get('LOCAL_ES_HOSTS'),
        )

    def open_spider(self, spider):
        self.es_client = Elasticsearch(hosts="http://127.0.0.1:9200")

    def close_spider(self, spider):
        self.es_client.close()

    def process_item(self, item, spider):
        if isinstance(item, TrajectoryItem):
            address = item["address"]
            trajectories = item["trajectory"]
            address_location = []
            trajectory_location = []
            res_location = []
            for date in trajectories:
                location_list = self.handle_geocoder(trajectories[date])
                for location in location_list[1]:
                    res_location.append(location)
                for location in location_list[0]:
                    combat_str = time.strftime("%Y-%m-%d %H:%M:%S", date) + '@' + location
                    trajectory_location.append(combat_str)

            for ads in address:
                ads_list = self.handle_geocoder(ads)
                for location in ads_list[1]:
                    res_location.append(location)
                address_location.append(ads_list[0])

            item['address_location'] = address_location
            item['trajectory_location'] = trajectory_location
            item['location'] = res_location
            self.store_item(item)
        return item

    def similarity(self, a, b):
        return SequenceMatcher(None, a, b).ratio()

    def handle_geocoder(self, actions):
        split_action = re.split(pattern=',|。|，|、|；|;', string=actions)
        res_list = [[], []]
        for action in split_action:
            m = 15.0
            if "公交" in action:
                m = 0.0
            match_str = action.replace("（", "(")
            query_str = match_str.split("(")[0]
            dsl = {
                "query": {
                    "match": {
                        "message": {
                            "query": query_str,
                            "analyzer": "ik_max_word"
                        }
                    }
                }
            }

            poi_result = self.es_client.search(index='wuhanpoinew', body=dsl)
            poi_hits_num = poi_result['hits']['total']['value']

            num_of_calculation = 9
            if poi_hits_num < 9:
                num_of_calculation = poi_hits_num - 1

            max_score = 0.0
            longitude = 0.0
            latitude = 0.0
            while num_of_calculation >= 0:

                poi_score = poi_result['hits']['hits'][num_of_calculation]['_score']
                poi_address = poi_result['hits']['hits'][num_of_calculation]['_source']['address']
                poi_name = poi_result['hits']['hits'][num_of_calculation]['_source']['name']
                poi_adname = poi_result['hits']['hits'][num_of_calculation]['_source']['adname']
                cur_score_decimal = self.similarity(match_str, poi_address) + self.similarity(match_str,
                                                                                              poi_name) + self.similarity(
                    match_str, poi_adname)
                cur_score = poi_score * cur_score_decimal
                print(cur_score)
                if cur_score > max_score:
                    longitude = poi_result['hits']['hits'][num_of_calculation]['_source']['locationx']
                    latitude = poi_result['hits']['hits'][num_of_calculation]['_source']['locationy']
                    max_score = cur_score
                num_of_calculation = num_of_calculation - 1
            if longitude != 0.0 and max_score > 10.0:
                res_list[0].append(str(longitude) + "," + str(latitude))
                res_list[1].append(str(latitude) + "," + str(longitude))
        return res_list

    def store_item(self, item):
        dsl = {'address': item['address'], 'address_location': item['address_location'],
               'extra_info': item['extra_info'], 'trajectory': '',
               'trajectory_location': item['trajectory_location'], 'location': item['location']}
        for trajectory in item['trajectory_list']:
            dsl['trajectory'] += trajectory + "\n"
        self.es_client.index(index=self.index, body=dsl)

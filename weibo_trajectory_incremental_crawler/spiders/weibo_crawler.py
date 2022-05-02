import hashlib
import json
import re
import time
import uuid
import redis
import scrapy
from scrapy import Request
from selenium import webdriver
from selenium.webdriver.common.by import By

from weibo_trajectory_incremental_crawler.items import WeiboItem
from pyquery import PyQuery as pq

class WeiboCrawlerSpider(scrapy.Spider):
    name = 'weibo_crawler'
    allowed_domains = ['m.weibo.cn']
    # start_urls = ['http://m.weibo.cn/']
    test_id = 0
    options = webdriver.ChromeOptions()
    # options.set_headless(True)
    options.add_argument("--headless")  # 设置谷歌为headless无界面模式
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(chrome_options=options, executable_path="/home/pengjiahao/module/scrapy/chromedriver")
    # 用户
    user_url = 'https://m.weibo.cn/api/container/getIndex?uid={uid}&type=uid&value={uid}&containerid=100505{uid}'
    # 微博
    weibo_url = 'https://m.weibo.cn/api/container/getIndex?uid={uid}&type=uid&page={page}&containerid=107603{uid}'
    # 关注
    follow_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_{uid}&page={page}'
    # 粉丝     注意 粉丝页码参数是since_id=,而不是关注页码中page=
    fan_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{uid}&since_id={page}'

    split_tag = "@@@###"
    start_uids = [
        '2759348142',  # 武汉发布
    ]

    def start_requests(self):
        self.r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        for uid in self.start_uids:
            yield Request(self.weibo_url.format(uid=uid, page=1), callback=self.parse_weibos,
                          meta={'page': 1, 'uid': uid})

    def parse_weibos(self, response):

        result = json.loads(response.text)
        if result.get('ok') == 1 and result.get('data').get('cards'):
            weibos = result.get('data').get('cards')
            for weibo in weibos:
                mblog = weibo.get('mblog')
                # 判断是否存在mblog，有时不存在
                if mblog:
                    weibo_item = WeiboItem()

                    weibo_item['id'] = mblog.get('id')  # 微博id
                    weibo_item['idstr'] = mblog.get('idstr')
                    weibo_item['edit_count'] = mblog.get('edit_count')
                    weibo_item['created_at'] = mblog.get('created_at')
                    weibo_item['version'] = mblog.get('version')
                    weibo_item['thumbnail_pic'] = mblog.get('thumbnail_pic')
                    weibo_item['bmiddle_pic'] = mblog.get('bmiddle_pic')
                    weibo_item['original_pic'] = mblog.get('original_pic')
                    weibo_item['source'] = mblog.get('source')
                    weibo_item['user'] = response.meta.get('uid')  # 用户id

                    # 检测有没有阅读全文:
                    all_text = mblog.get('text')
                    if '>#武汉疫情#<' in all_text or '>#情况通报#<' in all_text or '>#疫情通报#<' in all_text:
                        if '>全文<' in all_text:

                            all_text_url = 'https://m.weibo.cn/statuses/extend?id=' + mblog.get('id')
                            yield Request(all_text_url, callback=self.parse_all_text, meta={'item': weibo_item})

                        else:
                            # 文本超链接深度挖掘数据
                            a = pq(all_text)('a')
                            href = a.eq(a.length - 1).attr('href')
                            text = str(pq(mblog.get('text'))).replace("<br/>", self.split_tag)
                            text = pq(text).text()
                            text = ''.join([x.strip() for x in text])
                            if 'ttarticle' in href:
                                text = ""
                                self.driver.get(href)
                                time.sleep(5)
                                # self.driver.get_screenshot_as_file(str(uuid.uuid1())+".png")
                                elements = self.driver.find_elements(by=By.XPATH,
                                                                     value="//div[@class=\"WB_editor_iframe_new\"]/p")
                                for e in elements:
                                    text = text + e.text + self.split_tag
                                elements = self.driver.find_elements(by=By.XPATH,
                                                                     value="//div[@class=\"WB_editor_iframe_new\"]/p/span")
                                for e in elements:
                                    text = text + e.text
                            weibo_item['text'] = text + self.split_tag
                            h = hashlib.md5()  # 生成一个md5 hash对象
                            h.update(weibo_item['text'].encode())  # 对字符串s进行加密更新处理
                            if self.r.setnx(h.hexdigest(), 'flag'):
                                yield weibo_item

            # 下一页微博
            uid = response.meta.get('uid')
            page = response.meta.get('page') + 1
            if page < 6:
                yield Request(self.weibo_url.format(uid=uid, page=page), callback=self.parse_weibos,
                          meta={'uid': uid, 'page': page})

    # 有阅读全文的情况，获取全文
    def parse_all_text(self, response):
        result = json.loads(response.text)
        if result.get('ok') and result.get('data'):
            weibo_item = response.meta['item']

            all_text = result.get('data').get('longTextContent')
            # 文本超链接深度挖掘数据
            a = pq(all_text)('a')
            href = a.eq(a.length - 1).attr('href')

            text = str(pq(all_text)).replace("<br/>", self.split_tag)
            text = pq(text).text()
            text = ''.join([x.strip() for x in text])

            if 'ttarticle' in href:
                text = ""
                self.driver.get(href)
                time.sleep(5)
                # self.driver.get_screenshot_as_file(str(uuid.uuid1())+".png")
                elements = self.driver.find_elements(by=By.XPATH,
                                                     value="//div[@class=\"WB_editor_iframe_new\"]/p")
                for e in elements:
                    text = text + e.text + self.split_tag
                elements = self.driver.find_elements(by=By.XPATH,
                                                     value="//div[@class=\"WB_editor_iframe_new\"]/p/span")
                for e in elements:
                    text = text + e.text + self.split_tag

            weibo_item['text'] = text
            h = hashlib.md5()  # 生成一个md5 hash对象
            h.update(weibo_item['text'].encode())  # 对字符串s进行加密更新处理
            if self.r.setnx(h.hexdigest(), 'flag'):
                yield weibo_item

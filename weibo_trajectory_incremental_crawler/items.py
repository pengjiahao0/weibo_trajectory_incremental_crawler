# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Item, Field


class WeiboItem(Item):
    collection = 'weibos'

    id = Field()
    idstr = Field()
    edit_count = Field()
    created_at = Field()
    version = Field()
    thumbnail_pic = Field()
    bmiddle_pic = Field()
    original_pic = Field()
    source = Field()
    user = Field()
    text = Field()
    crawled_at = Field()


class TrajectoryItem(Item):
    collections = 'trajectories'

    id = Field()
    trajectory = Field()
    trajectory_list = Field()
    address = Field()
    address_location = Field()
    trajectory_location = Field()
    extra_info = Field()
    location = Field()


class SpacialLocationItem:
    collections = 'spaciallocations'

    date = Field()
    location = Field()


class UserItem(Item):
    collection = 'users'

    id = Field()  # 用户id
    name = Field()  # 昵称
    profile_image = Field()  # 头像图片
    cover_image = Field()  # 背景图片
    verified_reason = Field()  # 认证
    description = Field()  # 简介
    fans_count = Field()  # 粉丝数
    follows_count = Field()  # 关注数
    weibos_count = Field()  # 微博数
    mbrank = Field()  # 会员等级
    verified = Field()  # 是否认证
    verified_type = Field()  # 认证类型
    verified_type_ext = Field()  # 以下不知道
    gender = Field()
    mbtype = Field()
    urank = Field()
    crawled_at = Field()  # 抓取时间戳 在pipelines.py中


class UserRelationItem(Item):
    collection = 'UserRelation'

    id = Field()
    follows = Field()
    fans = Field()


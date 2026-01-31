# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CaoliuItem(scrapy.Item):
    """草榴帖子数据模型"""
    # 帖子URL
    url = scrapy.Field()
    # 分配的视频ID (video_01, video_02...)
    video_id = scrapy.Field()
    # 影片名称
    title = scrapy.Field()
    # 图片URL列表（最多5张）
    image_urls = scrapy.Field()
    # 下载后的图片路径
    images = scrapy.Field()
    # 下载链接（占位，后续完成）
    download_link = scrapy.Field()
    # 视频下载量（从列表页获取）
    download_count = scrapy.Field()
    # 图片下载是否成功（Pipeline内部使用）
    download_success = scrapy.Field()

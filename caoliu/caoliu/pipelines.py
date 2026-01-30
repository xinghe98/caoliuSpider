# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from scrapy import Request
from scrapy.exceptions import DropItem
import os
import csv


class CaoliuIndexPipeline:
    """
    为每个帖子分配唯一的video_id，并维护CSV索引文件
    这个Pipeline必须在ImagesPipeline之前运行
    """
    
    def __init__(self, download_dir):
        self.download_dir = download_dir
        self.video_counter = 0
        self.csv_file = None
        self.csv_writer = None
    
    @classmethod
    def from_crawler(cls, crawler):
        download_dir = crawler.settings.get('CAOLIU_DOWNLOAD_DIR', './downloads')
        return cls(download_dir)
    
    def open_spider(self, spider):
        """爬虫启动时，初始化CSV文件"""
        # 确保目录存在
        os.makedirs(self.download_dir, exist_ok=True)
        
        # 查找已存在的最大video编号
        self.video_counter = self._get_max_video_index()
        
        # 打开CSV文件（追加模式）
        csv_path = os.path.join(self.download_dir, 'index.csv')
        file_exists = os.path.exists(csv_path)
        
        self.csv_file = open(csv_path, 'a', newline='', encoding='utf-8-sig')
        self.csv_writer = csv.writer(self.csv_file)
        
        # 如果是新文件，写入表头
        if not file_exists:
            self.csv_writer.writerow(['video_id', 'title', 'download_link'])
    
    def _get_max_video_index(self):
        """获取已存在的最大video编号"""
        max_index = 0
        images_dir = os.path.join(self.download_dir, 'images')
        
        if os.path.exists(images_dir):
            for folder_name in os.listdir(images_dir):
                if folder_name.startswith('video_'):
                    try:
                        index = int(folder_name.split('_')[1])
                        max_index = max(max_index, index)
                    except (ValueError, IndexError):
                        pass
        
        return max_index
    
    def process_item(self, item, spider):
        """为每个item分配video_id"""
        self.video_counter += 1
        video_id = f'video_{self.video_counter:02d}'
        item['video_id'] = video_id
        
        # 写入CSV
        self.csv_writer.writerow([
            video_id,
            item.get('title', ''),
            item.get('download_link', '')
        ])
        self.csv_file.flush()  # 实时写入
        
        spider.logger.info(f"分配ID: {video_id} -> {item.get('title', '')[:30]}...")
        
        return item
    
    def close_spider(self, spider):
        """爬虫关闭时，关闭CSV文件"""
        if self.csv_file:
            self.csv_file.close()


class CaoliuImagesPipeline(ImagesPipeline):
    """自定义图片下载Pipeline，按video_id分文件夹保存"""
    
    def get_media_requests(self, item, info):
        """生成图片下载请求"""
        image_urls = item.get('image_urls', [])
        video_id = item.get('video_id', 'unknown')
        
        for idx, image_url in enumerate(image_urls):
            yield Request(
                url=image_url,
                meta={
                    'video_id': video_id,
                    'image_index': idx + 1
                }
            )
    
    def file_path(self, request, response=None, info=None, *, item=None):
        """自定义图片保存路径: video_id/image_01.jpg"""
        video_id = request.meta.get('video_id', 'unknown')
        image_index = request.meta.get('image_index', 1)
        
        # 从URL获取图片扩展名
        url = request.url
        ext = url.split('.')[-1].split('?')[0].lower()
        if ext not in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
            ext = 'jpg'
        
        return f'{video_id}/image_{image_index:02d}.{ext}'
    
    def item_completed(self, results, item, info):
        """图片下载完成后的回调"""
        image_paths = [x['path'] for ok, x in results if ok]
        item['images'] = image_paths
        
        if not image_paths:
            info.spider.logger.warning(f"未能下载任何图片: {item.get('video_id')}")
        
        return item


class CaoliuPipeline:
    """最终处理Pipeline"""
    
    def process_item(self, item, spider):
        return item

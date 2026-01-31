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
import shutil


class CaoliuIndexPipeline:
    """
    为每个帖子分配唯一的video_id
    注意：不在此处写入CSV，等图片下载成功后再写入
    """
    
    def __init__(self, download_dir):
        self.download_dir = download_dir
        self.video_counter = 0
    
    @classmethod
    def from_crawler(cls, crawler):
        download_dir = crawler.settings.get('CAOLIU_DOWNLOAD_DIR', './downloads')
        return cls(download_dir)
    
    def open_spider(self, spider):
        """爬虫启动时，初始化"""
        # 确保目录存在
        os.makedirs(self.download_dir, exist_ok=True)
        
        # 查找已存在的最大video编号
        self.video_counter = self._get_max_video_index()
        spider.logger.info(f"当前最大video编号: {self.video_counter}")
    
    def _get_max_video_index(self):
        """获取已存在的最大video编号"""
        max_index = 0
        
        # 检查下载目录下的所有 video_* 文件夹
        if os.path.exists(self.download_dir):
            for folder_name in os.listdir(self.download_dir):
                if folder_name.startswith('video_'):
                    try:
                        index = int(folder_name.split('_')[1])
                        max_index = max(max_index, index)
                    except (ValueError, IndexError):
                        pass
        
        return max_index
    
    def process_item(self, item, spider):
        """为每个item分配video_id（不写入CSV）"""
        self.video_counter += 1
        video_id = f'video_{self.video_counter:02d}'
        item['video_id'] = video_id
        
        spider.logger.info(f"分配ID: {video_id} -> {item.get('title', '')[:30]}...")
        
        return item


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
        
        # 标记下载是否成功
        item['download_success'] = len(image_paths) > 0
        
        if not image_paths:
            info.spider.logger.warning(f"未能下载任何图片: {item.get('video_id')} - {item.get('title', '')[:30]}")
        else:
            info.spider.logger.info(f"成功下载 {len(image_paths)} 张图片: {item.get('video_id')}")
        
        return item


class CaoliuFinalPipeline:
    """
    最终处理Pipeline
    - 只有图片下载成功的item才写入CSV
    - 下载失败的item删除其文件夹并丢弃
    """
    
    def __init__(self, download_dir):
        self.download_dir = download_dir
        self.csv_file = None
        self.csv_writer = None
        self.success_count = 0
        self.fail_count = 0
    
    @classmethod
    def from_crawler(cls, crawler):
        download_dir = crawler.settings.get('CAOLIU_DOWNLOAD_DIR', './downloads')
        return cls(download_dir)
    
    def open_spider(self, spider):
        """爬虫启动时，打开CSV文件"""
        csv_path = os.path.join(self.download_dir, 'index.csv')
        file_exists = os.path.exists(csv_path)
        
        self.csv_file = open(csv_path, 'a', newline='', encoding='utf-8-sig')
        self.csv_writer = csv.writer(self.csv_file)
        
        # 如果是新文件，写入表头
        if not file_exists:
            self.csv_writer.writerow(['video_id', 'title', 'download_link', 'download_count', 'image_count'])
    
    def process_item(self, item, spider):
        """处理item，只有下载成功的才写入CSV"""
        video_id = item.get('video_id', 'unknown')
        download_success = item.get('download_success', False)
        
        if download_success:
            # 下载成功，写入CSV
            self.csv_writer.writerow([
                video_id,
                item.get('title', ''),
                item.get('download_link', ''),
                item.get('download_count', ''),
                len(item.get('images', []))
            ])
            self.csv_file.flush()  # 实时写入
            self.success_count += 1
            
            spider.logger.info(f"✓ 保存成功: {video_id} -> {item.get('title', '')[:30]}...")
            return item
        else:
            # 下载失败，删除已创建的文件夹（如果存在）
            folder_path = os.path.join(self.download_dir, video_id)
            if os.path.exists(folder_path):
                try:
                    shutil.rmtree(folder_path)
                    spider.logger.info(f"✗ 已删除失败的文件夹: {folder_path}")
                except Exception as e:
                    spider.logger.error(f"删除文件夹失败 {folder_path}: {e}")
            
            self.fail_count += 1
            spider.logger.warning(f"✗ 丢弃失败项: {video_id} -> {item.get('title', '')[:30]}...")
            
            # 抛出异常丢弃此item
            raise DropItem(f"图片下载失败，已丢弃: {video_id}")
    
    def close_spider(self, spider):
        """爬虫关闭时，关闭CSV文件并输出统计"""
        if self.csv_file:
            self.csv_file.close()
        
        spider.logger.info(f"="*50)
        spider.logger.info(f"爬取完成统计:")
        spider.logger.info(f"  成功: {self.success_count} 个")
        spider.logger.info(f"  失败: {self.fail_count} 个")
        spider.logger.info(f"="*50)

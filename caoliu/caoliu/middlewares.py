# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import HtmlResponse
import logging

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

# Cloudflare 保护的图床域名列表
CLOUDFLARE_PROTECTED_DOMAINS = [
    'tu.ymawv.la',
    'ymawv.la',
]

logger = logging.getLogger(__name__)


class CaoliuSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # matching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class CaoliuDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class CloudflareBypassMiddleware:
    """
    处理 Cloudflare 保护的图床域名
    使用 cloudscraper 绕过 JavaScript Challenge
    """
    
    def __init__(self):
        self.scraper = None
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls()
    
    def _get_scraper(self):
        """懒加载 cloudscraper 实例"""
        if self.scraper is None:
            try:
                import cloudscraper
                self.scraper = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'windows',
                        'mobile': False
                    }
                )
                logger.info("CloudScraper 初始化成功")
            except ImportError:
                logger.warning("cloudscraper 未安装，无法绕过 Cloudflare 保护")
                return None
            except Exception as e:
                logger.error(f"CloudScraper 初始化失败: {e}")
                return None
        return self.scraper
    
    def _is_cloudflare_domain(self, url):
        """检查 URL 是否属于 Cloudflare 保护的域名"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        for protected_domain in CLOUDFLARE_PROTECTED_DOMAINS:
            if protected_domain in domain:
                return True
        return False
    
    def process_request(self, request, spider):
        """
        对 Cloudflare 保护的域名使用 cloudscraper 处理
        """
        if not self._is_cloudflare_domain(request.url):
            return None  # 非 Cloudflare 域名，继续正常处理
        
        scraper = self._get_scraper()
        if scraper is None:
            logger.warning(f"无法处理 Cloudflare 保护的 URL: {request.url}")
            return None
        
        try:
            logger.debug(f"使用 CloudScraper 下载: {request.url}")
            
            # 使用 cloudscraper 发起请求
            response = scraper.get(
                request.url,
                timeout=30,
                headers={
                    'Referer': 't66y.com',
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                }
            )
            
            # 构造 Scrapy Response
            from scrapy.http import Response
            return Response(
                url=request.url,
                status=response.status_code,
                headers=dict(response.headers),
                body=response.content,
                request=request,
            )
            
        except Exception as e:
            logger.error(f"CloudScraper 请求失败 {request.url}: {e}")
            return None
    
    def spider_opened(self, spider):
        spider.logger.info("CloudflareBypassMiddleware 已启用")

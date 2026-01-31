import scrapy
import re
from urllib.parse import urlparse, parse_qs
from caoliu.items import CaoliuItem


class CaoliuSpider(scrapy.Spider):
    name = "caoliu"
    allowed_domains = ["t66y.com"]

    # 爬取的页数范围（可在settings中通过CAOLIU_MAX_PAGES配置）
    start_page = 20
    max_page = 5
    
    # 最低下载量阈值（从settings读取）
    min_download_count = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """从crawler获取settings配置"""
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.min_download_count = crawler.settings.getint('CAOLIU_MIN_DOWNLOAD_COUNT', 0)
        return spider

    def start_requests(self):
        """生成多页的起始请求"""
        self.logger.info(f"最低下载量阈值: {self.min_download_count}")
        base_url = "https://t66y.com/thread0806.php?fid=25&search=&page={}"
        for page in range(self.start_page, self.start_page + self.max_page):
            url = base_url.format(page)
            self.logger.info(f"请求第 {page} 页: {url}")
            yield scrapy.Request(url=url, callback=self.parse, meta={"page": page})

    def parse(self, response):
        """
        解析一级页面，提取帖子列表中的链接
        XPath: //*[@id="tbody"] 下的每一个 href
        """
        page = response.meta.get("page", 1)
        self.logger.info(f"正在解析第 {page} 页...")

        # 提取帖子列表中的所有链接
        posts = response.xpath('//*[@id="tbody"]//tr')
        
        skipped_count = 0

        for post in posts:
            # 帖子标题和链接在 h3/a 或 td/a 中
            link = post.xpath(".//td[2]//h3/a/@href | .//td[2]/a/@href").get()
            title = post.xpath(".//td[2]//h3/a/text() | .//td[2]/a/text()").get()
            
            # 提取下载量 - 位于第5列 (td[5])
            download_count_text = post.xpath(".//td[5]/text()").get()
            download_count = None
            if download_count_text:
                download_count_text = download_count_text.strip()
                # 处理可能是 '--' 或空的情况
                if download_count_text.isdigit():
                    download_count = int(download_count_text)

            if link:
                # 检查下载量是否满足阈值
                if self.min_download_count > 0:
                    if download_count is None or download_count < self.min_download_count:
                        skipped_count += 1
                        self.logger.debug(
                            f"跳过低下载量帖子: {title[:30]}... (下载量: {download_count}, 阈值: {self.min_download_count})"
                        )
                        continue
                
                # 构建完整的URL
                full_url = response.urljoin(link)
                self.logger.info(f"发现帖子: {title} -> {full_url}, 下载量: {download_count}")

                # 跳转到二级页面进行详情解析
                yield scrapy.Request(
                    url=full_url, 
                    callback=self.parse_detail, 
                    meta={"list_title": title, "download_count": download_count}
                )
        
        if skipped_count > 0:
            self.logger.info(f"第 {page} 页跳过 {skipped_count} 个低下载量帖子")

    def parse_detail(self, response):
        """
        解析二级页面，提取详细信息
        内容区域: //*[@id="conttpc"]
        """
        item = CaoliuItem()

        # 帖子URL
        item["url"] = response.url

        # 1. 影片名称 - 从内容区域提取，通常是第一行文本或标题
        content_div = response.xpath('//*[@id="conttpc"]')

        # 尝试多种方式获取影片名称
        # 方式1: 内容区第一个文本节点
        title = content_div.xpath(".//text()").get()
        if title:
            title = title.strip()

        # 如果内容区没找到，使用列表页的标题
        if not title:
            title = response.meta.get("list_title", "")

        # 清理标题，去除常见前缀
        title = self._clean_title(title.strip() if title else "")
        item["title"] = title
        # 从内容区域提取所有图片
        all_images = content_div.xpath(".//img/@src | .//img/@ess-data").getall()

        # 过滤并清洗图片URL
        valid_images = []
        for img_url in all_images:
            if img_url and img_url.startswith("http"):
                # 去重
                if img_url not in valid_images:
                    valid_images.append(img_url)
            elif img_url and not img_url.startswith("http"):
                # 处理相对路径
                full_img_url = response.urljoin(img_url)
                if full_img_url not in valid_images:
                    valid_images.append(full_img_url)

        # 最多取前5张
        item["image_urls"] = valid_images[:5]

        # 3. 下载链接 - 从rmdown URL提取hash并构造magnet链接
        # XPath: //*[@id="rmlink"]
        rmdown_link = response.xpath('//*[@id="rmlink"]/@href').get()
        magnet_link = self._extract_magnet(rmdown_link)
        item["download_link"] = magnet_link

        # 4. 下载量 - 从列表页传递过来
        item["download_count"] = response.meta.get("download_count")

        self.logger.info(
            f"解析完成: {item['title']}, 图片数: {len(item['image_urls'])}, "
            f"下载量: {item['download_count']}, magnet: {magnet_link is not None}"
        )

        yield item

    def _clean_title(self, title):
        """清理影片名称，去除常见前缀"""
        if not title:
            return ""

        # 需要去除的前缀列表
        prefixes = [
            "【影片名称】：",
            "【影片名称】:",
            "【影片名稱】：",
            "【影片名稱】:",
            "【影片名称】",
            "【影片名稱】",
            "[影片名称]：",
            "[影片名称]:",
            "[影片名称]",
            "影片名称：",
            "影片名称:",
            "影片名稱：",
            "影片名稱:",
        ]

        for prefix in prefixes:
            if title.startswith(prefix):
                title = title[len(prefix) :].strip()
                break

        return title

    def _extract_magnet(self, rmdown_url):
        """
        从rmdown.com URL提取hash并构造magnet链接
        URL格式: https://www.rmdown.com/link.php?hash=xxxxx
        Magnet格式: magnet:?xt=urn:btih:xxxxx

        注意: rmdown的hash前面有额外字符（如版本号），真正的InfoHash是最后40位
        """
        if not rmdown_url:
            return None

        try:
            # 方式1: 从URL参数提取hash
            parsed = urlparse(rmdown_url)
            params = parse_qs(parsed.query)

            if "hash" in params:
                hash_value = params["hash"][0]
                # rmdown的hash比标准InfoHash长，取最后40位
                if len(hash_value) >= 40:
                    infohash = hash_value[-40:]  # 只取最后40位
                    if re.match(r"^[0-9a-fA-F]{40}$", infohash):
                        return f"magnet:?xt=urn:btih:{infohash}"

            # 方式2: 使用正则匹配URL中的hash，取最后40位
            match = re.search(r"hash=([0-9a-fA-F]+)", rmdown_url)
            if match:
                hash_value = match.group(1)
                if len(hash_value) >= 40:
                    infohash = hash_value[-40:]
                    return f"magnet:?xt=urn:btih:{infohash}"

        except Exception as e:
            self.logger.warning(f"提取magnet链接失败: {e}")

        return None

import random
import time
import os
import zipfile
import logging
import sqlite3
import shutil
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta

from DrissionPage import ChromiumPage, ChromiumOptions
from RecaptchaSolver import RecaptchaSolver

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """数据库管理器：记录已爬取的页面"""

    def __init__(self, db_path: str = "crawler_data.db"):
        self.db_path = db_path
        self.conn = None
        self._init_database()

    def _init_database(self):
        """初始化数据库"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()

            # 创建已爬取URL表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crawled_urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    main_title TEXT,
                    detail_title TEXT,
                    download_path TEXT,
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'success',
                    error_message TEXT
                )
            ''')

            # 创建翻页进度表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS page_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    base_url TEXT,
                    current_page INTEGER DEFAULT 1,
                    total_pages INTEGER DEFAULT 3339,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON crawled_urls(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_page ON page_progress(base_url)')

            self.conn.commit()
            logger.info(f"数据库初始化完成: {self.db_path}")

        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise

    def is_url_crawled(self, url: str) -> bool:
        """检查URL是否已爬取"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1 FROM crawled_urls WHERE url = ?', (url,))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"检查URL失败: {e}")
            return False

    def mark_url_crawled(self, url: str, main_title: str = "", detail_title: str = "",
                         download_path: str = "", status: str = "success", error_message: str = ""):
        """标记URL为已爬取"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO crawled_urls 
                (url, main_title, detail_title, download_path, status, error_message) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (url, main_title, detail_title, download_path, status, error_message))
            self.conn.commit()
            logger.debug(f"标记URL为已爬取: {url}")
        except Exception as e:
            logger.error(f"标记URL失败: {e}")

    def get_crawled_stats(self) -> Dict:
        """获取爬取统计信息"""
        try:
            cursor = self.conn.cursor()

            cursor.execute('SELECT COUNT(*) FROM crawled_urls')
            total = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM crawled_urls WHERE status = "success"')
            success = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM crawled_urls WHERE status = "failed"')
            failed = cursor.fetchone()[0]

            cursor.execute('SELECT MAX(crawl_time) FROM crawled_urls')
            last_crawl = cursor.fetchone()[0]

            return {
                "total": total,
                "success": success,
                "failed": failed,
                "last_crawl": last_crawl
            }
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}

    def get_page_progress(self,
                          base_url: str = "https://www.creativefabrica.com/embroidery/embroidery-designs/") -> Dict:
        """获取翻页进度"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                'SELECT current_page, total_pages, last_updated FROM page_progress WHERE base_url = ?',
                (base_url,)
            )
            result = cursor.fetchone()

            if result:
                return {
                    "current_page": result[0],
                    "total_pages": result[1],
                    "last_updated": result[2]
                }

            # 如果没有记录，插入初始记录
            cursor.execute('''
                INSERT INTO page_progress (base_url, current_page, total_pages) 
                VALUES (?, 1, 3339)
            ''', (base_url,))
            self.conn.commit()

            return {
                "current_page": 1,
                "total_pages": 3339,
                "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            logger.error(f"获取翻页进度失败: {e}")
            return {"current_page": 1, "total_pages": 3339, "last_updated": ""}

    def update_page_progress(self, base_url: str, current_page: int):
        """更新翻页进度"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO page_progress (base_url, current_page, total_pages) 
                VALUES (?, ?, 3339)
            ''', (base_url, current_page))
            self.conn.commit()
            logger.debug(f"更新翻页进度: {base_url} -> 第 {current_page} 页")
        except Exception as e:
            logger.error(f"更新翻页进度失败: {e}")

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")


class ZipExtractor:
    """ZIP文件解压器"""

    @staticmethod
    def extract_zip(zip_path: str, extract_to: str = None) -> bool:
        """
        解压ZIP文件到指定目录，解压后删除原压缩包

        参数:
            zip_path: ZIP文件路径
            extract_to: 解压目标目录，如果为None则解压到同名文件夹

        返回:
            bool: 是否解压成功
        """
        if not os.path.exists(zip_path):
            logger.error(f"ZIP文件不存在: {zip_path}")
            return False

        try:
            # 确定解压目录
            if extract_to is None:
                # 创建同名文件夹（去掉.zip扩展名）
                base_name = os.path.basename(zip_path)
                folder_name = os.path.splitext(base_name)[0]
                extract_to = os.path.join(os.path.dirname(zip_path), folder_name)

            # 创建解压目录
            os.makedirs(extract_to, exist_ok=True)

            logger.info(f"开始解压: {zip_path} -> {extract_to}")

            # 解压文件
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # 获取文件列表
                file_list = zip_ref.namelist()
                logger.info(f"ZIP文件中包含 {len(file_list)} 个文件")

                # 解压所有文件
                zip_ref.extractall(extract_to)

            logger.info(f"解压完成: {extract_to}")

            # 删除原压缩包
            try:
                os.remove(zip_path)
                logger.info(f"已删除原压缩包: {zip_path}")
            except Exception as e:
                logger.warning(f"删除原压缩包失败 {zip_path}: {e}")

            return True

        except zipfile.BadZipFile:
            logger.error(f"ZIP文件损坏或不是有效的ZIP文件: {zip_path}")
            return False
        except Exception as e:
            logger.error(f"解压ZIP文件失败 {zip_path}: {e}")
            return False

    @staticmethod
    def find_zip_files(directory: str) -> List[str]:
        """
        查找目录中的所有ZIP文件

        参数:
            directory: 要搜索的目录

        返回:
            ZIP文件路径列表
        """
        zip_files = []

        if not os.path.exists(directory):
            return zip_files

        for file in os.listdir(directory):
            if file.lower().endswith('.zip'):
                zip_files.append(os.path.join(directory, file))

        return zip_files


class FileBackupManager:
    """文件备份管理器 - 简化版：直接复制文件夹"""

    def __init__(self, backup_base_path: str = "D:/自动化数据/刺绣图备份"):
        self.backup_base_path = backup_base_path

    def create_backup(self, source_dir: str) -> bool:
        """
        创建文件夹备份 - 简化版：直接复制整个文件夹

        参数:
            source_dir: 源目录路径

        返回:
            bool: 是否备份成功
        """
        try:
            # 获取源目录的相对路径（相对于下载根目录）
            download_root = "D:/自动化数据/刺绣图"

            # 确保源目录在下载根目录下
            if not source_dir.startswith(download_root):
                logger.warning(f"源目录不在下载根目录下: {source_dir}")
                return False

            # 计算相对路径
            relative_path = os.path.relpath(source_dir, download_root)

            # 构建备份路径（保持相同的文件夹结构）
            backup_path = os.path.join(self.backup_base_path, relative_path)

            # 如果备份目录已存在，先删除旧的
            if os.path.exists(backup_path):
                try:
                    shutil.rmtree(backup_path)
                    logger.info(f"删除旧的备份: {backup_path}")
                except Exception as e:
                    logger.warning(f"删除旧备份失败: {e}")

            # 复制整个文件夹
            if os.path.exists(source_dir) and os.path.isdir(source_dir):
                # 确保父目录存在
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)

                # 复制整个目录树
                shutil.copytree(source_dir, backup_path)

                # 统计复制的文件数量
                file_count = sum([len(files) for _, _, files in os.walk(backup_path)])
                logger.info(f"备份完成: {source_dir} -> {backup_path} (共 {file_count} 个文件)")
                return True
            else:
                logger.warning(f"源目录不存在或不是目录: {source_dir}")
                return False

        except Exception as e:
            logger.error(f"创建备份失败: {e}")
            return False


class BrowserManager:
    """管理浏览器初始化和配置"""

    CHROME_ARGUMENTS = [
        "-no-first-run",
        "-force-color-profile=srgb",
        "-metrics-recording-only",
        "-password-store=basic",
        "-use-mock-keychain",
        "-export-tagged-pdf",
        "-no-default-browser-check",
        "-disable-background-mode",
        "-enable-features=NetworkService,NetworkServiceInProcess",
        "-disable-features=FlashDeprecationWarning",
        "-deny-permission-prompts",
        "-disable-gpu",
        "-accept-lang=en-US",
        "--disable-usage-stats",
        "--disable-crash-reporter",
        "--no-sandbox"
    ]

    @staticmethod
    def create_driver() -> Tuple[ChromiumPage, RecaptchaSolver]:
        """创建并配置浏览器驱动"""
        options = ChromiumOptions()
        for argument in BrowserManager.CHROME_ARGUMENTS:
            options.set_argument(argument)

        driver = ChromiumPage(addr_or_opts=options)
        recaptcha_solver = RecaptchaSolver(driver)

        logger.info("浏览器驱动创建成功")
        return driver, recaptcha_solver


class AntiAntiCrawler:
    """反反爬虫模块：智能等待和随机化 - 优化版提高速度"""

    def __init__(self):
        # 核心等待时间配置（秒）- 优化提高速度
        self.WAIT_CONFIG = {
            "page_load": (3, 8),  # 页面加载等待 - 缩短
            "element_find": (2, 5),  # 元素查找等待 - 缩短
            "between_actions": (4, 10),  # 动作间等待 - 大幅缩短
            "between_pages": (10, 20),  # 页面间等待 - 大幅缩短
            "download_timeout": 60,  # 下载超时 - 缩短
            "captcha_solve": 10,  # 验证码解决时间 - 缩短
        }

        # 模式切换：白天/夜晚不同频率
        self.day_mode_multiplier = 1.0  # 白天模式
        self.night_mode_multiplier = 0.8  # 夜晚模式（稍微降低频率）

        # 行为随机化参数
        self.random_actions = [
            "scroll_up_down",  # 滚动页面
            "move_mouse",  # 移动鼠标
            "short_pause",  # 短暂停顿
            "change_viewport",  # 改变视窗大小
        ]

        # 计数器和状态
        self.request_count = 0
        self.session_start_time = time.time()

        # 优化：降低长时间休息频率
        self.max_continuous_hours = 12  # 最大连续运行小时数增加到12小时
        self.big_break_hours = 1  # 长时间休息小时数减少到1小时

        # 新增：短期休息配置
        self.short_break_interval = 50  # 每50个项目休息一次
        self.short_break_duration = (30, 60)  # 短期休息30-60秒

        logger.info("反反爬虫模块初始化完成（优化速度版）")

    def get_current_mode_multiplier(self) -> float:
        """获取当前模式乘数（基于时间）"""
        current_hour = datetime.now().hour

        # 夜晚模式：凌晨0点到早上7点
        if 0 <= current_hour <= 7:
            return self.night_mode_multiplier
        # 高峰时段：上午9-12点，下午2-6点
        elif (9 <= current_hour <= 12) or (14 <= current_hour <= 18):
            return 1.2  # 稍微降低频率
        else:
            return self.day_mode_multiplier

    def smart_wait(self, wait_type: str = "between_actions",
                   min_time: float = None, max_time: float = None) -> float:
        """
        智能随机等待，支持多种等待策略

        参数:
            wait_type: 等待类型
            min_time: 最小等待时间（覆盖配置）
            max_time: 最大等待时间（覆盖配置）

        返回:
            实际等待的时间
        """
        # 获取当前模式乘数
        multiplier = self.get_current_mode_multiplier()

        # 根据请求数量动态调整等待时间（降低增加幅度）
        request_factor = 1.0 + (self.request_count // 200) * 0.1  # 每200个请求增加10%
        request_factor = min(request_factor, 2.0)  # 最大2.0倍

        # 确定等待时间范围
        if wait_type in self.WAIT_CONFIG:
            if isinstance(self.WAIT_CONFIG[wait_type], tuple):
                config_min, config_max = self.WAIT_CONFIG[wait_type]
            else:
                config_min = config_max = self.WAIT_CONFIG[wait_type]
        else:
            config_min, config_max = self.WAIT_CONFIG["between_actions"]

        # 使用传入的参数或配置
        if min_time is None:
            min_time = config_min
        if max_time is None:
            max_time = config_max

        # 应用乘数和因子
        min_time = min_time * multiplier * request_factor
        max_time = max_time * multiplier * request_factor

        # 降低随机性：80%时间在范围内，20%时间稍长
        if random.random() < 0.2:
            max_time = max_time * 1.5

        wait_time = random.uniform(min_time, max_time)

        # 记录请求计数
        self.request_count += 1

        # 每50个请求输出统计
        if self.request_count % 50 == 0:
            elapsed_hours = (time.time() - self.session_start_time) / 3600
            requests_per_hour = self.request_count / elapsed_hours if elapsed_hours > 0 else 0
            logger.info(f"请求统计: 总数={self.request_count}, 运行时间={elapsed_hours:.1f}小时, "
                        f"平均={requests_per_hour:.1f}请求/小时, 当前乘数={multiplier:.2f}")

        logger.debug(f"智能等待 ({wait_type}): {wait_time:.1f} 秒 "
                     f"(min={min_time:.1f}, max={max_time:.1f}, multiplier={multiplier:.2f})")

        # 执行等待
        time.sleep(wait_time)

        return wait_time

    def random_behavior(self):
        """执行随机行为，模拟人类操作"""
        if random.random() < 0.2:  # 降低到20%概率执行随机行为
            action = random.choice(self.random_actions)

            if action == "scroll_up_down":
                # 模拟滚动
                scroll_amount = random.randint(200, 800)
                scroll_direction = random.choice([-1, 1])
                logger.debug(f"模拟滚动: {scroll_direction * scroll_amount}px")
                time.sleep(random.uniform(0.5, 1.5))

            elif action == "move_mouse":
                # 模拟鼠标移动
                logger.debug("模拟鼠标移动")
                time.sleep(random.uniform(0.3, 1.0))

            elif action == "short_pause":
                # 短暂停顿
                pause_time = random.uniform(0.5, 2.0)
                logger.debug(f"模拟短暂停顿: {pause_time:.1f}秒")
                time.sleep(pause_time)

            elif action == "change_viewport":
                # 模拟改变视窗
                logger.debug("模拟改变视窗大小")
                time.sleep(random.uniform(0.5, 1.0))

    def check_long_run_protection(self, items_processed: int) -> bool:
        """
        检查是否需要长时间休息

        参数:
            items_processed: 已处理的项目数

        返回:
            bool: 是否需要休息
        """
        elapsed_hours = (time.time() - self.session_start_time) / 3600

        # 检查连续运行时间
        if elapsed_hours >= self.max_continuous_hours:
            logger.warning(f"已连续运行 {elapsed_hours:.1f} 小时，达到最大连续运行时间，需要长时间休息")
            return True

        # 检查处理数量 - 增加处理数量间隔
        if items_processed % 500 == 0 and items_processed > 0:
            logger.info(f"已处理 {items_processed} 个项目，建议休息")

            # 降低休息概率到40%
            if random.random() < 0.4:
                return True

        return False

    def take_big_break(self):
        """执行长时间休息 - 缩短休息时间"""
        break_hours = self.big_break_hours + random.uniform(0, 0.5)  # 1-1.5小时
        break_seconds = break_hours * 3600

        logger.warning(f"执行长时间休息: {break_hours:.1f} 小时 ({break_seconds:.0f} 秒)")
        logger.warning(f"休息开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.warning(
            f"预计恢复时间: {(datetime.now() + timedelta(seconds=break_seconds)).strftime('%Y-%m-%d %H:%M:%S')}")

        time.sleep(break_seconds)

        # 重置会话开始时间
        self.session_start_time = time.time()
        logger.info("长时间休息结束，恢复爬取")

    def check_short_break(self, items_processed: int) -> bool:
        """
        检查是否需要短期休息

        参数:
            items_processed: 已处理的项目数

        返回:
            bool: 是否需要短期休息
        """
        if items_processed % self.short_break_interval == 0 and items_processed > 0:
            logger.info(f"已处理 {items_processed} 个项目，进行短期休息")
            return True
        return False

    def take_short_break(self):
        """执行短期休息"""
        break_duration = random.uniform(*self.short_break_duration)
        logger.info(f"执行短期休息: {break_duration:.1f} 秒")
        time.sleep(break_duration)


class CreativeFabricaScraper:
    """Creative Fabrica 网站爬虫核心类"""

    def __init__(self, driver: ChromiumPage, recaptcha_solver: RecaptchaSolver):
        self.driver = driver
        self.recaptcha_solver = recaptcha_solver
        self.db_manager = DatabaseManager()  # 数据库管理器
        self.zip_extractor = ZipExtractor()  # ZIP解压器
        self.anti_anti = AntiAntiCrawler()  # 反反爬虫模块
        self.backup_manager = FileBackupManager()  # 备份管理器

        # 配置常量
        self.BASE_DOWNLOAD_PATH = "D:/自动化数据/刺绣图"
        # 基础URL和总页数
        self.BASE_URL = "https://www.creativefabrica.com/embroidery/embroidery-designs/"
        self.TOTAL_PAGES = 3339  # 总页数

        # 重试配置
        self.RETRY_CONFIG = {
            "max_retries": 3,  # 减少重试次数
            "retry_delay": 3  # 减少重试延迟
        }

        # 跳过已爬取的URL
        self.skip_crawled = True

        # 进度跟踪
        self.total_processed = 0

        logger.info(f"爬虫初始化完成，总页数: {self.TOTAL_PAGES}")

    def smart_wait(self, wait_type: str = "between_actions",
                   min_time: float = None, max_time: float = None) -> float:
        """智能随机等待（包装反反爬虫模块）"""
        # 首先执行可能的随机行为
        self.anti_anti.random_behavior()

        # 执行智能等待
        wait_time = self.anti_anti.smart_wait(wait_type, min_time, max_time)

        return wait_time

    def get_main_page_title(self) -> str:
        """获取主页面标题并清理格式"""
        try:
            title_element = self.driver.ele(
                'xpath://h1[@class="text-[29px] font-bold mb-2 text-center md:text-left"]'
            )
            if title_element:
                title_text = title_element.texts()[0]
                cleaned_title = title_text.replace(" ", "").replace("\n", "")
                logger.info(f"获取到主标题: {cleaned_title}")
                return cleaned_title
            return ""
        except Exception as e:
            logger.error(f"获取主标题失败: {e}")
            return ""

    def get_subpage_links_from_page(self, page_url: str, skip_crawled: bool = True) -> List[str]:
        """
        从指定页面URL获取所有子页面链接

        参数:
            page_url: 页面URL
            skip_crawled: 是否跳过已爬取的链接

        返回:
            链接列表
        """
        try:
            logger.info(f"访问页面: {page_url}")
            self.driver.get(page_url)
            self.driver.wait.doc_loaded()
            self.smart_wait("page_load")

            # 等待页面完全加载
            time.sleep(1)  # 减少等待时间

            link_elements = self.driver.eles(
                'xpath://a[@data-testid="product-card-title"]'
            )

            links = []
            for link_element in link_elements:
                href = link_element.attr('href')
                if href and href not in links:
                    links.append(href)

            logger.info(f"从页面获取到 {len(links)} 个链接")

            # 如果跳过已爬取的链接，则进行过滤
            if skip_crawled and self.db_manager:
                new_links = []
                crawled_count = 0

                for url in links:
                    if self.db_manager.is_url_crawled(url):
                        crawled_count += 1
                    else:
                        new_links.append(url)

                logger.info(f"链接过滤: 总共 {len(links)} 个，已爬取 {crawled_count} 个，未爬取 {len(new_links)} 个")
                return new_links

            return links

        except Exception as e:
            logger.error(f"从页面获取链接失败 {page_url}: {e}")
            return []

    def get_detail_page_title(self) -> str:
        """获取详情页标题"""
        try:
            title_element = self.driver.ele('xpath://h1[@id="product-title"]')
            return title_element.texts()[0] if title_element else ""
        except Exception as e:
            logger.error(f"获取详情页标题失败: {e}")
            return ""

    def get_breadcrumb_paths(self) -> Tuple[str, str, str]:
        """
        获取面包屑导航路径

        返回:
            Tuple[一级子路径, 二级子路径, 三级子路径]
        """
        try:
            # 获取面包屑导航元素
            breadcrumb_elements = self.driver.eles('xpath://ul[@class="c-breadcrumb__list"]/li')

            # 提取各级路径
            level1 = ""
            level2 = ""
            level3 = ""

            if len(breadcrumb_elements) > 1:
                level1_element = breadcrumb_elements[1]  # li[2]
                level1 = level1_element.texts()[0] if level1_element.texts() else ""

            if len(breadcrumb_elements) > 2:
                level2_element = breadcrumb_elements[2]  # li[3]
                level2 = level2_element.texts()[0] if level2_element.texts() else ""

            if len(breadcrumb_elements) > 3:
                level3_element = breadcrumb_elements[3]  # li[4]
                level3 = level3_element.texts()[0] if level3_element.texts() else ""

            logger.info(f"获取面包屑导航: 一级={level1}, 二级={level2}, 三级={level3}")
            return level1, level2, level3

        except Exception as e:
            logger.error(f"获取面包屑导航失败: {e}")
            return "", "", ""

    def create_download_directory(self, detail_title_en: str) -> str:
        """
        创建下载目录（基于面包屑导航）- 不翻译版本

        参数:
            detail_title_en: 详情页标题（英文）

        返回:
            完整下载路径
        """
        try:
            # 获取面包屑导航路径
            level1, level2, level3 = self.get_breadcrumb_paths()

            # 清理文件名中的无效字符
            def clean_filename(filename):
                invalid_chars = '<>:"/\\|?*'
                for char in invalid_chars:
                    filename = filename.replace(char, '_')
                filename = filename.replace('\n', '_').replace('\r', '_').replace('\t', '_')
                filename = filename.strip('. ')
                if len(filename) > 100:
                    filename = filename[:100]
                return filename

            # 构建路径部分
            path_parts = []

            # 一级路径
            if level1 and level1.strip():
                level1_clean = clean_filename(level1)
                path_parts.append(level1_clean)
                logger.info(f"一级路径: {level1_clean}")

            # 二级路径
            if level2 and level2.strip():
                level2_clean = clean_filename(level2)
                path_parts.append(level2_clean)
                logger.info(f"二级路径: {level2_clean}")

            # 三级路径
            if level3 and level3.strip():
                level3_clean = clean_filename(level3)
                path_parts.append(level3_clean)
                logger.info(f"三级路径: {level3_clean}")

            # 如果面包屑导航为空，使用详情标题
            if not path_parts:
                logger.warning("面包屑导航为空，使用详情标题作为路径")
                detail_title_clean = clean_filename(detail_title_en)
                path_parts.append(detail_title_clean)
                logger.info(f"详情标题路径: {detail_title_clean}")

            # 构建完整路径
            download_path = os.path.join(self.BASE_DOWNLOAD_PATH, *path_parts)

            # 创建目录
            os.makedirs(download_path, exist_ok=True)
            logger.info(f"创建下载目录: {download_path}")

            return download_path

        except Exception as e:
            logger.error(f"创建目录失败: {e}")

            # 备用方案：使用时间戳
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_path = os.path.join(self.BASE_DOWNLOAD_PATH, f"Unnamed_{timestamp}")
            os.makedirs(safe_path, exist_ok=True)
            logger.info(f"使用备用目录: {safe_path}")
            return safe_path

    def download_main_image(self, download_path: str) -> bool:
        """下载主展示图片"""
        try:
            img_element = self.driver.ele('xpath://div[contains(@class, "fotorama__active")]/img')
            if img_element:
                img_element.save(path=download_path, name="preview.jpg")
                logger.info(f"主图片已保存到: {os.path.join(download_path, 'preview.jpg')}")
                return True
            return False
        except Exception as e:
            logger.error(f"下载主图片失败: {e}")
            return False

    def download_pdf(self, download_path: str) -> bool:
        """下载PDF文件"""
        try:
            pdf_element = self.driver.ele('xpath://a[@class="c-button c-button--grey-purple u-mb-20"]')
            if pdf_element:
                pdf_url = pdf_element.attr('href')
                if pdf_url:
                    self.driver.download(file_url=pdf_url, save_path=download_path, rename="instructions.pdf")
                    logger.info(f"PDF已保存到: {os.path.join(download_path, 'instructions.pdf')}")
                    return True
            return False
        except Exception as e:
            logger.error(f"下载PDF失败: {e}")
            return False

    def solve_captcha_and_click_submit(self) -> bool:
        """
        解决验证码并点击提交按钮

        返回:
            bool: 是否成功点击提交按钮
        """
        try:
            logger.info("开始解决验证码")
            t0 = time.time()

            try:
                self.recaptcha_solver.solveCaptcha()
            except Exception as e:
                logger.error(f"验证码解决过程中抛出异常: {e}")
                # 不做任何操作，等待2秒
                logger.info("验证码解决异常，等待2秒")
                time.sleep(2)

            solve_time = time.time() - t0
            logger.info(f"验证码处理完成，总耗时: {solve_time:.2f}秒")

            # 查找提交按钮
            submit_button = self.driver.ele(
                'xpath://button[@class="btn c-button c-button--green c-button--md u-mt-10 u-mb-10 u-semibold" and @type="submit"]'
            )

            if submit_button:
                logger.info("找到提交按钮，点击提交")
                submit_button.click()
                return True
            else:
                logger.warning("未找到提交按钮")
                return False

        except Exception as e:
            logger.error(f"验证码解决流程失败: {e}")
            return False

    def download_zip_file(self, download_path: str, subpage_url: str) -> Tuple[bool, Optional[str]]:
        """
        下载ZIP压缩文件（修改验证码处理逻辑）

        返回:
            Tuple[是否成功, ZIP文件路径（如果成功）]
        """
        max_retries = self.RETRY_CONFIG["max_retries"]

        for attempt in range(max_retries):
            try:
                logger.info(f"下载ZIP文件尝试 {attempt + 1}/{max_retries}")

                self.driver.wait.doc_loaded()
                self.smart_wait("page_load")

                # 查找下载按钮
                download_selectors = [
                    'xpath://a[contains(@class, "download-link") and contains(@class, "c-button--green")]',
                    'xpath://a[contains(@class, "product-download-button")]',
                    'xpath://a[contains(text(), "Download") and contains(@class, "c-button")]'
                ]

                download_button = None
                for selector in download_selectors:
                    download_button = self.driver.ele(selector, timeout=8)  # 减少超时时间
                    if download_button:
                        logger.info(f"使用选择器找到下载按钮: {selector}")
                        break

                if not download_button:
                    logger.warning("未找到下载按钮")

                    # 检查是否有验证码 - 使用更精确的验证码检测
                    captcha_selectors = [
                        'xpath://div[contains(@class, "recaptcha")]',
                        'xpath://div[contains(@class, "g-recaptcha")]',
                        'xpath://iframe[contains(@title, "reCAPTCHA")]'
                    ]

                    has_captcha = False
                    for captcha_selector in captcha_selectors:
                        if self.driver.ele(captcha_selector, timeout=2):  # 减少超时时间
                            has_captcha = True
                            logger.info("检测到验证码")
                            break

                    if has_captcha:
                        # 调用验证码解决函数
                        if self.solve_captcha_and_click_submit():
                            logger.info("验证码已解决并点击提交按钮，等待下载开始")
                            # 记录当前文件列表，然后等待新文件出现
                            before_files = set(os.listdir(download_path)) if os.path.exists(download_path) else set()

                            # 等待下载完成，查找新出现的ZIP文件
                            zip_found = False
                            zip_file_path = None

                            for wait_attempt in range(30):  # 减少等待时间
                                time.sleep(1)

                                if os.path.exists(download_path):
                                    current_files = set(os.listdir(download_path))
                                    new_files = current_files - before_files

                                    zip_files = [f for f in new_files if f.lower().endswith('.zip')]

                                    if zip_files:
                                        zip_filename = zip_files[0]
                                        zip_file_path = os.path.join(download_path, zip_filename)
                                        logger.info(f"通过验证码解决后下载发现ZIP文件: {zip_filename}")
                                        zip_found = True
                                        break

                            if zip_found:
                                return True, zip_file_path
                            else:
                                logger.warning("验证码解决后等待30秒仍未检测到ZIP文件")
                                # 修改点：不进行重试，直接返回失败，保留已下载的其他文件
                                logger.info("跳过ZIP下载，保留已下载的主图和PDF文件")
                                return False, None

                        else:
                            logger.warning("验证码解决失败")

                    # 刷新页面后重试
                    if attempt < max_retries - 1:
                        logger.info("刷新页面后重试")
                        self.driver.refresh()
                        self.driver.wait.doc_loaded()
                        self.smart_wait("page_load")
                        continue

                # 记录下载前的文件列表
                before_files = set(os.listdir(download_path)) if os.path.exists(download_path) else set()

                # 尝试点击下载
                try:
                    logger.info("尝试点击下载按钮")
                    download_result = download_button.click.to_download(
                        save_path=download_path,
                        timeout=self.anti_anti.WAIT_CONFIG["download_timeout"]
                    )

                    # 情况1：正常下载
                    if download_result:
                        logger.info("ZIP文件下载成功启动")

                        # 等待下载完成，查找新出现的ZIP文件
                        zip_file_path = None
                        zip_found = False

                        for wait_attempt in range(30):  # 减少等待时间
                            time.sleep(1)

                            if os.path.exists(download_path):
                                current_files = set(os.listdir(download_path))
                                new_files = current_files - before_files

                                zip_files = [f for f in new_files if f.lower().endswith('.zip')]

                                if zip_files:
                                    zip_filename = zip_files[0]
                                    zip_file_path = os.path.join(download_path, zip_filename)
                                    logger.info(f"发现新的ZIP文件: {zip_filename}")
                                    zip_found = True
                                    break

                        if zip_found:
                            return True, zip_file_path
                        else:
                            logger.warning("未检测到ZIP文件被创建")
                            # 继续重试

                    # 情况2和3：click.to_download返回False
                    else:
                        logger.warning("click.to_download返回False")

                        # 检查是否跳转到验证码页面
                        captcha_selectors = [
                            'xpath://div[contains(@class, "recaptcha")]',
                            'xpath://div[contains(@class, "g-recaptcha")]',
                            'xpath://iframe[contains(@title, "reCAPTCHA")]'
                        ]

                        has_captcha = False
                        for captcha_selector in captcha_selectors:
                            if self.driver.ele(captcha_selector, timeout=2):  # 减少超时时间
                                has_captcha = True
                                logger.info("检测到验证码")
                                break

                        if has_captcha:
                            # 调用验证码解决函数
                            if self.solve_captcha_and_click_submit():
                                logger.info("验证码已解决并点击提交按钮，等待下载开始")
                                # 验证码解决后直接进入下载等待流程
                                # 记录当前文件列表，然后等待新文件出现
                                before_files = set(os.listdir(download_path)) if os.path.exists(
                                    download_path) else set()

                                # 等待下载完成，查找新出现的ZIP文件
                                zip_found = False
                                zip_file_path = None

                                for wait_attempt in range(30):  # 减少等待时间
                                    time.sleep(1)

                                    if os.path.exists(download_path):
                                        current_files = set(os.listdir(download_path))
                                        new_files = current_files - before_files

                                        zip_files = [f for f in new_files if f.lower().endswith('.zip')]

                                        if zip_files:
                                            zip_filename = zip_files[0]
                                            zip_file_path = os.path.join(download_path, zip_filename)
                                            logger.info(f"通过验证码解决后下载发现ZIP文件: {zip_filename}")
                                            zip_found = True
                                            break

                                if zip_found:
                                    return True, zip_file_path
                                else:
                                    logger.warning("验证码解决后等待30秒仍未检测到ZIP文件")
                                    # 修改点：不进行重试，直接返回失败，保留已下载的其他文件
                                    logger.info("跳过ZIP下载，保留已下载的主图和PDF文件")
                                    return False, None

                            else:
                                logger.warning("验证码解决失败")
                                # 刷新页面后继续循环
                                if attempt < max_retries - 1:
                                    self.driver.refresh()
                                    self.driver.wait.doc_loaded()
                                    self.smart_wait("page_load")
                                    continue
                        else:
                            # 情况2：需要重新定位下载按钮
                            logger.info("页面没有验证码，重新定位下载按钮")
                            # 刷新页面后继续循环
                            if attempt < max_retries - 1:
                                self.driver.refresh()
                                self.driver.wait.doc_loaded()
                                self.smart_wait("page_load")
                                continue

                except Exception as click_error:
                    logger.warning(f"点击下载按钮时出错: {click_error}")

                    if "ElementLostError" in str(click_error):
                        logger.info("元素失效，重新尝试")
                        if attempt < max_retries - 1:
                            self.driver.refresh()
                            self.driver.wait.doc_loaded()
                            self.smart_wait("between_actions")
                            continue

                    # 检查是否有验证码
                    captcha_selectors = [
                        'xpath://div[contains(@class, "recaptcha")]',
                        'xpath://div[contains(@class, "g-recaptcha")]',
                        'xpath://iframe[contains(@title, "reCAPTCHA")]'
                    ]

                    has_captcha = False
                    for captcha_selector in captcha_selectors:
                        if self.driver.ele(captcha_selector, timeout=2):  # 减少超时时间
                            has_captcha = True
                            logger.info("检测到验证码")
                            break

                    if has_captcha:
                        # 调用验证码解决函数
                        if self.solve_captcha_and_click_submit():
                            logger.info("验证码已解决并点击提交按钮，等待下载开始")
                            # 验证码解决后直接进入下载等待流程
                            # 记录当前文件列表，然后等待新文件出现
                            before_files = set(os.listdir(download_path)) if os.path.exists(download_path) else set()

                            # 等待下载完成，查找新出现的ZIP文件
                            zip_found = False
                            zip_file_path = None

                            for wait_attempt in range(30):  # 减少等待时间
                                time.sleep(1)

                                if os.path.exists(download_path):
                                    current_files = set(os.listdir(download_path))
                                    new_files = current_files - before_files

                                    zip_files = [f for f in new_files if f.lower().endswith('.zip')]

                                    if zip_files:
                                        zip_filename = zip_files[0]
                                        zip_file_path = os.path.join(download_path, zip_filename)
                                        logger.info(f"通过验证码解决后下载发现ZIP文件: {zip_filename}")
                                        zip_found = True
                                        break

                            if zip_found:
                                return True, zip_file_path
                            else:
                                logger.warning("验证码解决后等待30秒仍未检测到ZIP文件")
                                # 修改点：不进行重试，直接返回失败，保留已下载的其他文件
                                logger.info("跳过ZIP下载，保留已下载的主图和PDF文件")
                                return False, None

                        else:
                            logger.warning("验证码解决失败")

                    # 备用下载方法：获取href直接下载
                    logger.info("尝试备用下载方法：获取href直接下载")
                    try:
                        download_url = download_button.attr('href')
                        if download_url and download_url.startswith('http'):
                            logger.info(f"直接下载URL: {download_url}")

                            # 记录下载前的文件列表
                            before_files = set(os.listdir(download_path)) if os.path.exists(download_path) else set()

                            # 直接下载
                            self.driver.download(file_url=download_url, save_path=download_path)

                            # 等待并查找新文件
                            zip_found = False
                            zip_file_path = None

                            for wait_attempt in range(30):  # 减少等待时间
                                time.sleep(1)
                                if os.path.exists(download_path):
                                    current_files = set(os.listdir(download_path))
                                    new_files = current_files - before_files
                                    zip_files = [f for f in new_files if f.lower().endswith('.zip')]

                                    if zip_files:
                                        zip_filename = zip_files[0]
                                        zip_file_path = os.path.join(download_path, zip_filename)
                                        logger.info(f"通过直接下载发现ZIP文件: {zip_filename}")
                                        zip_found = True
                                        break

                            if zip_found:
                                return True, zip_file_path
                            else:
                                logger.warning("直接下载未检测到ZIP文件")

                    except Exception as direct_error:
                        logger.warning(f"直接下载失败: {direct_error}")

            except Exception as e:
                error_type = type(e).__name__
                logger.error(f"下载尝试{attempt + 1}失败 ({error_type}): {e}")

                if attempt == max_retries - 1:
                    logger.error(f"ZIP文件下载失败，已达最大重试次数")
                    return False, None

                if "ElementLostError" in str(e):
                    logger.info("元素失效，重新访问页面")
                    self.driver.get(subpage_url)
                    self.driver.wait.doc_loaded()
                else:
                    logger.info("刷新当前页面")
                    self.driver.refresh()
                    self.driver.wait.doc_loaded()

                self.smart_wait("between_actions",
                                self.RETRY_CONFIG["retry_delay"],
                                self.RETRY_CONFIG["retry_delay"] * 2)

        return False, None

    def process_subpage(self, subpage_url: str, main_title_en: str, item_count: int, total_count: int) -> bool:
        """处理单个子页面（修改下载顺序）"""
        logger.info(f"处理项目 {item_count}/{total_count}: {subpage_url}")

        try:
            # 检查是否已爬取（双重检查）
            if self.skip_crawled and self.db_manager.is_url_crawled(subpage_url):
                logger.info(f"跳过已爬取的项目: {subpage_url}")
                return True  # 视为成功，但实际跳过

            # 访问子页面
            self.driver.get(subpage_url)
            self.driver.wait.doc_loaded()
            self.smart_wait("page_load")

            # 获取详情页标题
            detail_title_en = self.get_detail_page_title()
            if not detail_title_en:
                logger.warning(f"无法获取详情页标题: {subpage_url}")
                # 标记为失败
                self.db_manager.mark_url_crawled(
                    subpage_url,
                    status="failed",
                    error_message="无法获取详情页标题"
                )
                return False

            logger.info(f"详情页英文标题: {detail_title_en}")

            # 创建下载目录（基于面包屑导航）
            download_path = self.create_download_directory(detail_title_en)

            # 修改点1：先下载主图
            self.smart_wait("between_actions", 2, 5)  # 缩短等待时间
            img_success = self.download_main_image(download_path)
            logger.info(f"主图下载: {'成功' if img_success else '失败'}")

            # 修改点2：再下载PDF
            self.smart_wait("between_actions", 2, 5)  # 缩短等待时间
            pdf_success = self.download_pdf(download_path)
            logger.info(f"PDF下载: {'成功' if pdf_success else '失败'}")

            # 修改点3：最后下载ZIP文件
            self.smart_wait("between_actions", 2, 5)  # 缩短等待时间
            zip_success, zip_file_path = self.download_zip_file(download_path, subpage_url)

            # 判断整体成功情况
            success_count = sum([img_success, pdf_success, zip_success])
            status = "partial_success" if (
                                                      img_success or pdf_success) and not zip_success else "success" if success_count > 0 else "failed"

            logger.info(
                f"下载完成: 主图={'成功' if img_success else '失败'}, PDF={'成功' if pdf_success else '失败'}, ZIP={'成功' if zip_success else '失败'}")
            logger.info(f"总体状态: {status}")

            # 如果ZIP下载成功，则解压
            if zip_success and zip_file_path and os.path.exists(zip_file_path):
                logger.info(f"开始解压ZIP文件: {zip_file_path}")
                extract_success = self.zip_extractor.extract_zip(zip_file_path)

                if extract_success:
                    logger.info(f"ZIP文件解压成功并已删除原文件: {zip_file_path}")
                else:
                    logger.warning(f"ZIP文件解压失败: {zip_file_path}")
            elif not zip_success:
                logger.warning(f"ZIP文件下载失败，但保留已下载的主图和PDF文件")

            # 新增：创建文件备份（简化版：直接复制文件夹）
            if success_count > 0 and os.path.exists(download_path):
                try:
                    backup_success = self.backup_manager.create_backup(download_path)
                    if backup_success:
                        logger.info(f"文件备份成功: {download_path}")
                    else:
                        logger.warning("文件备份失败")
                except Exception as backup_error:
                    logger.error(f"创建备份时出错: {backup_error}")

            # 更新总处理计数
            self.total_processed += 1

            # 标记为爬取（根据状态）
            self.db_manager.mark_url_crawled(
                subpage_url,
                main_title=main_title_en,
                detail_title=detail_title_en,
                download_path=download_path,
                status=status,
                error_message="" if zip_success else "ZIP文件下载失败" if status == "partial_success" else "所有文件下载失败"
            )

            # 只要下载了至少一个文件，就返回True
            return success_count > 0

        except Exception as e:
            logger.error(f"处理子页面失败 {subpage_url}: {e}")

            # 标记为失败
            self.db_manager.mark_url_crawled(
                subpage_url,
                status="failed",
                error_message=str(e)[:200]
            )

            return False

    def run(self):
        """运行主爬虫流程 - 遍历网址方式翻页"""
        logger.info("=== Creative Fabrica 爬虫开始运行 ===")
        logger.info(f"目标数据量: 120,000 条")
        logger.info(f"总页数: {self.TOTAL_PAGES}")

        try:
            # 显示之前的统计信息
            stats = self.db_manager.get_crawled_stats()
            logger.info(
                f"数据库统计: 总共爬取 {stats.get('total', 0)} 个，成功 {stats.get('success', 0)} 个，失败 {stats.get('failed', 0)} 个")
            logger.info(f"剩余目标: {120000 - stats.get('total', 0):,} 个")

            # 获取翻页进度
            page_progress = self.db_manager.get_page_progress(self.BASE_URL)
            start_page = page_progress["current_page"]
            logger.info(f"从第 {start_page} 页开始，总页数: {self.TOTAL_PAGES}")

            # 访问第一页获取主标题（如果还没获取过）
            if start_page == 1:
                first_page_url = f"{self.BASE_URL}page/1/"
                logger.info(f"访问第一页获取主标题: {first_page_url}")
                self.driver.get(first_page_url)
                self.driver.wait.doc_loaded()
                self.smart_wait("page_load", 8, 12)  # 缩短等待时间

                # 获取主标题（英文）
                main_title_en = self.get_main_page_title()
                if not main_title_en:
                    logger.error("无法获取主页面标题，程序终止")
                    return

                logger.info(f"主页面英文标题: {main_title_en}")
            else:
                # 如果从中间开始，使用默认标题
                main_title_en = "Embroidery Designs"
                logger.info(f"从第 {start_page} 页继续，使用默认标题: {main_title_en}")

            # 遍历所有页面
            for page_num in range(start_page, self.TOTAL_PAGES + 1):
                try:
                    # 检查是否需要长时间休息
                    if self.anti_anti.check_long_run_protection(self.total_processed):
                        self.anti_anti.take_big_break()

                    # 检查是否需要短期休息
                    if self.anti_anti.check_short_break(self.total_processed):
                        self.anti_anti.take_short_break()

                    # 构建页面URL
                    if page_num == 1:
                        page_url = self.BASE_URL  # 第一页没有/page/1/
                    else:
                        page_url = f"{self.BASE_URL}page/{page_num}/"

                    logger.info(f"=== 处理第 {page_num}/{self.TOTAL_PAGES} 页 ===")

                    # 从页面获取链接
                    page_links = self.get_subpage_links_from_page(page_url, self.skip_crawled)

                    if not page_links:
                        logger.warning(f"第 {page_num} 页没有找到新的链接")
                        # 更新翻页进度
                        self.db_manager.update_page_progress(self.BASE_URL, page_num + 1)
                        continue

                    logger.info(f"第 {page_num} 页找到 {len(page_links)} 个新链接")

                    # 处理当前页的所有链接
                    successful_count = 0
                    total_links = len(page_links)

                    for idx, subpage_url in enumerate(page_links, 1):
                        success = self.process_subpage(subpage_url, main_title_en, idx, total_links)
                        if success:
                            successful_count += 1

                        # 显示进度
                        if idx % 10 == 0 or idx == total_links:  # 改为每10个显示一次进度
                            progress_percent = (idx / total_links) * 100
                            logger.info(
                                f"页面进度: {idx}/{total_links} ({progress_percent:.1f}%) | 成功: {successful_count}")

                        if idx < total_links:
                            wait_time = self.smart_wait("between_actions", 5, 12)  # 缩短等待时间
                            logger.debug(f"等待 {wait_time:.1f} 秒后处理下一个")

                    # 处理完当前页面
                    logger.info(f"第 {page_num} 页处理完成: 成功 {successful_count}/{total_links} 个")

                    # 更新翻页进度
                    self.db_manager.update_page_progress(self.BASE_URL, page_num + 1)

                    # 页面间等待（不同页面间等待更长时间）
                    if page_num < self.TOTAL_PAGES:
                        wait_time = self.smart_wait("between_pages", 15, 25)  # 缩短等待时间
                        logger.info(f"第 {page_num} 页处理完成，等待 {wait_time:.1f} 秒后处理下一页")

                        # 每处理20页，额外休息（减少频率）
                        if page_num % 20 == 0:
                            extra_rest = random.uniform(20, 60)  # 缩短额外休息时间
                            logger.info(f"已处理 {page_num} 页，额外休息 {extra_rest:.1f} 秒")
                            time.sleep(extra_rest)

                    # 显示总体进度
                    stats = self.db_manager.get_crawled_stats()
                    total_crawled = stats.get('total', 0)
                    progress_percent = (total_crawled / 120000) * 100

                    pages_remaining = self.TOTAL_PAGES - page_num
                    estimated_time_hours = (pages_remaining * 15) / 3600  # 假设每页15分钟（减少）

                    logger.info(f"总体进度: 已爬取 {total_crawled:,} 个，完成 {progress_percent:.1f}%")
                    logger.info(f"翻页进度: {page_num}/{self.TOTAL_PAGES} 页，剩余 {pages_remaining} 页")
                    logger.info(f"预计剩余时间: {estimated_time_hours:.1f} 小时")

                    # 计算当前速度
                    elapsed_hours = (time.time() - self.anti_anti.session_start_time) / 3600
                    if elapsed_hours > 0:
                        items_per_hour = self.total_processed / elapsed_hours
                        logger.info(f"当前速度: {items_per_hour:.1f} 个/小时")

                        # 预测每日爬取量
                        daily_items = items_per_hour * 20  # 假设每天运行20小时
                        logger.info(f"预测每日爬取量: {daily_items:.0f} 个/天")

                except Exception as e:
                    logger.error(f"处理第 {page_num} 页时出错: {e}")
                    # 更新翻页进度，以便下次继续
                    self.db_manager.update_page_progress(self.BASE_URL, page_num + 1)

                    # 等待后继续
                    self.smart_wait("between_pages", 20, 40)  # 缩短等待时间
                    continue

            # 输出最终总结
            logger.info("=== 爬虫运行完成 ===")
            stats = self.db_manager.get_crawled_stats()
            logger.info(
                f"最终统计: 总共爬取 {stats.get('total', 0):,} 个，成功 {stats.get('success', 0):,} 个，失败 {stats.get('failed', 0):,} 个")
            logger.info(f"完成进度: {stats.get('total', 0) / 120000 * 100:.1f}%")

            # 显示性能统计
            total_time_hours = (time.time() - self.anti_anti.session_start_time) / 3600
            overall_items_per_hour = stats.get('total', 0) / total_time_hours if total_time_hours > 0 else 0
            logger.info(f"总运行时间: {total_time_hours:.1f} 小时")
            logger.info(f"总体速度: {overall_items_per_hour:.1f} 个/小时")

            # 计算每日平均
            if total_time_hours > 0:
                daily_average = overall_items_per_hour * 24
                logger.info(f"24小时平均: {daily_average:.0f} 个/天")

        except KeyboardInterrupt:
            logger.info("用户中断程序执行")

            # 保存当前状态
            stats = self.db_manager.get_crawled_stats()
            logger.info(
                f"中断时统计: 总共爬取 {stats.get('total', 0):,} 个，完成进度: {stats.get('total', 0) / 120000 * 100:.1f}%")

        except Exception as e:
            logger.error(f"爬虫运行失败: {e}", exc_info=True)
        finally:
            # 关闭数据库连接
            self.db_manager.close()
            logger.info("程序结束")


def main():
    """主函数"""
    try:
        # 初始化浏览器
        driver, recaptcha_solver = BrowserManager.create_driver()

        # 创建爬虫实例并运行
        scraper = CreativeFabricaScraper(driver, recaptcha_solver)
        scraper.run()

    except Exception as e:
        logger.error(f"程序初始化失败: {e}", exc_info=True)
    finally:
        logger.info("浏览器会话结束")


if __name__ == '__main__':
    main()
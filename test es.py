# -*- coding: utf-8 -*-
import time
import logging
import re
import concurrent.futures
import requests
import feedparser
import socket
import urllib.parse
import json
import traceback
from threading import Lock
import hashlib
import pymysql
import random
from datetime import datetime

# ------------------------ 【新增：飞书机器人配置】 ------------------------
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/ec1480fe-6921-4e5b-86d2-53b8272ccc40"

def send_feishu_report(report_text):
    """
    发送飞书机器人文本报告
    """
    headers = {"Content-Type": "application/json"}
    data = {
        "msg_type": "text",
        "content": {
            "text": report_text
        }
    }
    try:
        resp = requests.post(FEISHU_WEBHOOK_URL, headers=headers, json=data, timeout=10)
        if resp.status_code != 200:
            logging.error(f"[Feishu] 发送失败，HTTP状态码={resp.status_code}, 响应={resp.text}")
        else:
            logging.info(f"[Feishu] 发送成功")
    except Exception as e:
        logging.error(f"[Feishu] 发送异常: {e}")
# ------------------------ 【飞书机器人配置结束】 ------------------------

# ------------------------
#   日志设置
# ------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ============== ES 处理类 ==============
class ElasticsearchHandler:
    """
    用于将 Apple/Spotify 爬虫抓取结果更新到 episode_ranking 索引，维持 status=0/1/2 逻辑
    """
    def __init__(self,
                 es_host="https://34.59.97.208:9200",
                 es_user='elastic',
                 es_password='IQJBoIJ=_YAlpVcU44By'):
        """初始化Elasticsearch连接"""
        from elasticsearch import Elasticsearch
        self.es = Elasticsearch(
            es_host,
            basic_auth=(es_user, es_password),
            verify_certs=False,  # 如果是自签名证书，需要关闭证书验证
            ssl_show_warn=False
        )
        self.index_name = "episode_ranking"

        if not self.es.ping():
            logger.error("无法连接到Elasticsearch")
            raise ConnectionError("无法连接到Elasticsearch")
        logger.info("成功连接到Elasticsearch")

    def update_es(self, data, chart_type, explicit_platform=None):
        """
        根据设计文档更新ES数据:
          1) 将当前平台+分类下 status=0 的记录更新为 status=1 (表示它们旧榜)
          2) 插入或更新本次新榜数据(status=0)
          3) 将剩余(仍为 status=1) 的记录更新为 status=2，并设置 ranking_end_time=当前时间
          4) 删除 status=2 且 ranking_end_time 超过72小时的记录
        """
        if not data:
            logger.warning("没有数据可保存到 ES")
            return 0

        # 确定平台 & 分类
        if explicit_platform:
            platform = explicit_platform
        else:
            # 如果 chart_type 中没有 "spotify" 则默认 apple
            platform = "apple" if "spotify" not in chart_type.lower() else "spotify"

        category = chart_type
        logger.info(f"开始更新平台[{platform}]类别[{category}]的数据, 共{len(data)}条记录")

        try:
            # 1) status=0 -> status=1
            changed_count = self.update_status_zero_to_one_for_category(platform, category)
            logger.info(f"将[{platform}][{category}]中 {changed_count} 条 status=0 改为 status=1")

            # 2) 插入或更新数据
            updated_count = 0
            for item in data:
                if self.insert_or_update_data(item, platform, category):
                    updated_count += 1

            logger.info(f"成功更新 {updated_count}/{len(data)} 条记录 (平台={platform},分类={category})")

            # 3) status=1 -> status=2
            changed_count2 = self.update_status_one_to_two_for_category(platform, category)
            logger.info(f"将[{platform}][{category}]中 {changed_count2} 条 status=1 改为 status=2 (下榜)")

            # 4) 删除 status=2 且 ranking_end_time 超过72小时
            deleted_count = self.clean_old_data()
            logger.info(f"删除下榜超过72小时的记录: {deleted_count} 条")

            # 最后强制刷新索引
            self.es.indices.refresh(index=self.index_name)

            # 检查是否还有卡在 status=1
            remain = self.check_and_fix_remaining_status_one(platform, category)
            if remain > 0:
                logger.warning(f"发现并修复了 {remain} 条卡在status=1的记录 (强制下榜)")

            return updated_count

        except Exception as e:
            logger.error(f"更新ES数据时出错: {e}")
            traceback.print_exc()
            # 尝试恢复
            self.fix_all_updating_records()
            return 0

    # ========== 更新过程中的辅助方法 ==========
    def update_status_zero_to_one_for_category(self, platform, category):
        """
        将特定平台+分类下, status=0 的记录全部置为1 (表示旧榜),
        以便新数据插入时设为 status=0
        """
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"ranking_platform": platform}},
                        {"term": {"ranking_platform_category": category}},
                        {"term": {"status": 0}}
                    ]
                }
            },
            "script": {
                "source": "ctx._source.status = 1"
            }
        }
        resp = self.es.update_by_query(index=self.index_name, body=query, refresh=True)
        return resp.get("updated", 0)

    def insert_or_update_data(self, item, platform, category):
        """
        如果 (platform, category, episode_id) 已存在:
            如果status=1 -> ranking/其他信息更新, status=0
            如果status=2 -> ranking/其他信息更新, status=0, ranking_start_time=now
        如果不存在 -> 新插入 status=0, ranking_start_time=now, ranking_end_time=-1

        注：es_id 来自外部索引匹配，若匹配不到则 item['es_id'] 可能是 None；这里存为 ""。
        """
        try:
            # es_id -> episode_id
            ep_id = item.get("es_id") or ""  # 若 None，转成空字符串
            now_ts = int(time.time())

            # 排名(尝试将 rank 转为 int)，若失败则默认999
            ranking = 999
            raw_rank = item.get("rank", 999)
            if isinstance(raw_rank, int):
                ranking = raw_rank
            elif isinstance(raw_rank, str) and raw_rank.isdigit():
                ranking = int(raw_rank)

            # 查找是否已存在
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"ranking_platform": platform}},
                            {"term": {"ranking_platform_category": category}},
                            {"term": {"episode_id": ep_id}}
                        ]
                    }
                }
            }
            search_resp = self.es.search(index=self.index_name, body=query)
            hits = search_resp["hits"]["hits"]
            if hits:
                # 已存在 -> update
                doc_id = hits[0]["_id"]
                old_doc = hits[0]["_source"]
                old_status = old_doc.get("status", -1)

                update_doc = {
                    "doc": {
                        "ranking": ranking,
                        "status": 0,  # 重新变为在榜
                    }
                }
                if old_status == 2:
                    update_doc["doc"]["ranking_start_time"] = now_ts
                    update_doc["doc"]["ranking_end_time"] = -1

                # 额外字段
                if item.get("url"):
                    update_doc["doc"]["episode_url"] = item["url"]
                if item.get("episode"):
                    update_doc["doc"]["episode_title"] = item["episode"]
                if item.get("rss_url"):
                    update_doc["doc"]["rss_url"] = item["rss_url"]
                if item.get("audio_url"):
                    update_doc["doc"]["audio_url"] = item["audio_url"]
                if item.get("channel_name"):
                    update_doc["doc"]["podcast_name"] = item["channel_name"]

                update_doc["doc"]["episode_id"] = ep_id  # 存储最终episode_id

                self.es.update(index=self.index_name, id=doc_id, body=update_doc, refresh=True)
                return True
            else:
                # 不存在 -> insert
                doc_body = {
                    "ranking_platform": platform,
                    "ranking_platform_category": category,
                    "episode_id": ep_id,
                    "ranking": ranking,
                    "ranking_start_time": now_ts,
                    "ranking_end_time": -1,
                    "status": 0,
                }
                if item.get("url"):
                    doc_body["episode_url"] = item["url"]
                if item.get("episode"):
                    doc_body["episode_title"] = item["episode"]
                if item.get("rss_url"):
                    doc_body["rss_url"] = item["rss_url"]
                if item.get("audio_url"):
                    doc_body["audio_url"] = item["audio_url"]
                if item.get("channel_name"):
                    doc_body["podcast_name"] = item["channel_name"]

                self.es.index(index=self.index_name, body=doc_body, refresh=True)
                return True

        except Exception as e:
            logger.error(f"插入/更新时出错: {e}, item={item}")
            return False

    def update_status_one_to_two_for_category(self, platform, category):
        """
        将特定平台+分类下, 仍为 status=1 的记录 -> status=2,
        并设置 ranking_end_time=now
        """
        now_ts = int(time.time())
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"ranking_platform": platform}},
                        {"term": {"ranking_platform_category": category}},
                        {"term": {"status": 1}}
                    ]
                }
            },
            "script": {
                "source": "ctx._source.status = 2; ctx._source.ranking_end_time = params.nowts",
                "params": {
                    "nowts": now_ts
                }
            }
        }
        resp = self.es.update_by_query(index=self.index_name, body=body, refresh=True)
        return resp.get("updated", 0)

    def clean_old_data(self):
        """
        删除 status=2 且 ranking_end_time 超过72小时的记录
        """
        now_ts = int(time.time())
        cutoff = now_ts - 72 * 3600
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"status": 2}},
                        {"range": {"ranking_end_time": {"lt": cutoff, "gt": 0}}}
                    ]
                }
            }
        }
        resp = self.es.delete_by_query(index=self.index_name, body=body, refresh=True)
        return resp.get("deleted", 0)

    def check_and_fix_remaining_status_one(self, platform, category):
        """
        防御性检查，如果仍有 status=1 记录(未被更新成0或2)，
        则强制改成 2 并设置 ranking_end_time=now
        """
        now_ts = int(time.time())
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"ranking_platform": platform}},
                        {"term": {"ranking_platform_category": category}},
                        {"term": {"status": 1}}
                    ]
                }
            },
            "script": {
                "source": "ctx._source.status = 2; ctx._source.ranking_end_time = params.nowts",
                "params": {
                    "nowts": now_ts
                }
            }
        }
        resp = self.es.update_by_query(index=self.index_name, body=body, refresh=True)
        return resp.get("updated", 0)

    def fix_all_updating_records(self):
        """
        紧急恢复: 将所有status=1记录改为status=2, end_time=now
        """
        now_ts = int(time.time())
        body = {
            "query": {
                "term": {"status": 1}
            },
            "script": {
                "source": "ctx._source.status = 2; ctx._source.ranking_end_time = params.nowts",
                "params": {
                    "nowts": now_ts
                }
            }
        }
        resp = self.es.update_by_query(index=self.index_name, body=body, refresh=True)
        fix_count = resp.get("updated", 0)
        if fix_count > 0:
            logger.warning(f"fix_all_updating_records: 修复{fix_count}条卡在status=1的记录")
        return fix_count


# ============== MySQL 处理类 ==============
class MySQLHandler:
    """
    将热榜数据同步到 MySQL 表：pod_episode_ranking
    表结构:
        CREATE TABLE pod_episode_ranking (
          id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          created_at datetime(3) DEFAULT NULL,
          updated_at datetime(3) DEFAULT NULL,
          deleted_at datetime(3) DEFAULT NULL,
          episode_id varchar(100) NOT NULL COMMENT '沐言自有数据体系下的episode_id',
          ranking_platform varchar(100) NOT NULL COMMENT 'apple/spotify',
          ranking_platform_category varchar(100) NOT NULL COMMENT '榜单类别',
          ranking int(11) DEFAULT '0' COMMENT '当前排名',
          ranking_start_time int(11) DEFAULT NULL COMMENT '上榜开始时间(UNIX时间戳)',
          ranking_end_time int(11) DEFAULT NULL COMMENT '下榜时间(UNIX时间戳). 在榜时为-1',
          status tinyint(1) NOT NULL DEFAULT '2' COMMENT '状态(0在榜,1更新中,2下榜)',
          PRIMARY KEY (id),
          KEY idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    def __init__(self,
                 host="82.156.188.147",
                 port=3306,
                 user="sync_episode_ranking",
                 password="jAQhTH3ZhrU1",
                 database="nex_wire_platform"):
        self.conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )
        logger.info("成功连接到 MySQL 数据库")

    def update_mysql(self, data, chart_type, explicit_platform=None):
        """
        与 Elasticsearch 的更新逻辑完全一致，只是写到 MySQL:
          1) 将当前平台+分类下 status=0 的记录更新为 status=1
          2) 插入或更新本次新榜数据(status=0)
          3) 将剩余(仍为 status=1) 的记录更新为 status=2，并设置 ranking_end_time=当前时间
          4) 删除 status=2 且 ranking_end_time 超过72小时的记录
        """
        if not data:
            logger.warning("没有数据可保存到 MySQL")
            return 0

        if explicit_platform:
            platform = explicit_platform
        else:
            platform = "apple" if "spotify" not in chart_type.lower() else "spotify"

        category = chart_type
        logger.info(f"[MySQL] 开始更新平台[{platform}]类别[{category}]的数据, 共{len(data)}条记录")

        try:
            # 1) status=0 -> status=1
            changed_count = self.update_status_zero_to_one_for_category(platform, category)
            logger.info(f"[MySQL] 将[{platform}][{category}]中 {changed_count} 条 status=0 改为 status=1")

            # 2) 插入或更新数据
            updated_count = 0
            for item in data:
                if self.insert_or_update_data(item, platform, category):
                    updated_count += 1

            logger.info(f"[MySQL] 成功更新 {updated_count}/{len(data)} 条记录")

            # 3) status=1 -> status=2
            changed_count2 = self.update_status_one_to_two_for_category(platform, category)
            logger.info(f"[MySQL] 将[{platform}][{category}]中 {changed_count2} 条 status=1 改为 status=2")

            # 4) 删除 status=2 且 end_time>72小时
            deleted_count = self.clean_old_data()
            logger.info(f"[MySQL] 删除下榜超过72小时的记录: {deleted_count} 条")

            self.conn.commit()

            # 检查是否还有卡在 status=1
            remain = self.check_and_fix_remaining_status_one(platform, category)
            if remain > 0:
                logger.warning(f"[MySQL] 发现并修复了 {remain} 条卡在status=1的记录 (强制下榜)")

            return updated_count

        except Exception as e:
            logger.error(f"[MySQL] 更新数据时出错: {e}")
            traceback.print_exc()
            self.conn.rollback()
            self.fix_all_updating_records()
            return 0

    def update_status_zero_to_one_for_category(self, platform, category):
        """将特定平台+分类下, status=0 的记录全部置为1"""
        with self.conn.cursor() as cur:
            sql = """
            UPDATE pod_episode_ranking
            SET status=1, updated_at=%s
            WHERE ranking_platform=%s
              AND ranking_platform_category=%s
              AND status=0
            """
            now_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            cur.execute(sql, (now_str, platform, category))
            return cur.rowcount

    def insert_or_update_data(self, item, platform, category):
        """
        若 (platform, category, episode_id) 已存在:
            - 若status=1 -> status=0, 更新ranking等
            - 若status=2 -> status=0, 更新ranking+start_time=now, end_time=-1
        若不存在 -> 插入 (status=0, start_time=now, end_time=-1)
        """
        ep_id = item.get("es_id") or ""  # None 转空
        if not ep_id:
            # 即使 ep_id 为空，也按逻辑存一份(episode_id="")
            pass

        # 排名
        ranking = 999
        raw_rank = item.get("rank", 999)
        if isinstance(raw_rank, int):
            ranking = raw_rank
        elif isinstance(raw_rank, str) and raw_rank.isdigit():
            ranking = int(raw_rank)

        now_ts = int(time.time())
        now_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        try:
            with self.conn.cursor() as cur:
                # 查是否已有
                sql_sel = """
                SELECT id, status
                FROM pod_episode_ranking
                WHERE ranking_platform=%s
                  AND ranking_platform_category=%s
                  AND episode_id=%s
                LIMIT 1
                """
                cur.execute(sql_sel, (platform, category, ep_id))
                row = cur.fetchone()

                if row:
                    # 更新
                    record_id = row["id"]
                    old_status = row["status"]

                    if old_status == 2:
                        sql_up = """
                        UPDATE pod_episode_ranking
                        SET ranking=%s,
                            status=0,
                            ranking_start_time=%s,
                            ranking_end_time=-1,
                            updated_at=%s
                        WHERE id=%s
                        """
                        cur.execute(sql_up, (ranking, now_ts, now_str, record_id))
                    else:
                        # old_status=0 or 1
                        sql_up = """
                        UPDATE pod_episode_ranking
                        SET ranking=%s,
                            status=0,
                            updated_at=%s
                        WHERE id=%s
                        """
                        cur.execute(sql_up, (ranking, now_str, record_id))
                    return True
                else:
                    # 插入
                    sql_ins = """
                    INSERT INTO pod_episode_ranking (
                        created_at, updated_at,
                        episode_id,
                        ranking_platform, ranking_platform_category,
                        ranking, ranking_start_time, ranking_end_time, status
                    ) VALUES (
                        %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, -1, 0
                    )
                    """
                    cur.execute(sql_ins, (now_str, now_str, ep_id,
                                          platform, category,
                                          ranking, now_ts))
                    return True

        except Exception as e:
            logger.error(f"[MySQL] 插入/更新时出错: {e}, item={item}")
            return False

    def update_status_one_to_two_for_category(self, platform, category):
        """将平台+分类下, status=1 -> status=2, ranking_end_time=now"""
        now_ts = int(time.time())
        now_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        with self.conn.cursor() as cur:
            sql = """
            UPDATE pod_episode_ranking
            SET status=2,
                ranking_end_time=%s,
                updated_at=%s
            WHERE ranking_platform=%s
              AND ranking_platform_category=%s
              AND status=1
            """
            cur.execute(sql, (now_ts, now_str, platform, category))
            return cur.rowcount

    def clean_old_data(self):
        """删除 status=2 且 ranking_end_time 超过72小时的记录"""
        now_ts = int(time.time())
        cutoff = now_ts - 72 * 3600
        with self.conn.cursor() as cur:
            sql = """
            DELETE FROM pod_episode_ranking
            WHERE status=2
              AND ranking_end_time > 0
              AND ranking_end_time < %s
            """
            cur.execute(sql, (cutoff,))
            return cur.rowcount

    def check_and_fix_remaining_status_one(self, platform, category):
        """若还有 status=1，强制改为2"""
        now_ts = int(time.time())
        now_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        with self.conn.cursor() as cur:
            sql = """
            UPDATE pod_episode_ranking
            SET status=2,
                ranking_end_time=%s,
                updated_at=%s
            WHERE ranking_platform=%s
              AND ranking_platform_category=%s
              AND status=1
            """
            cur.execute(sql, (now_ts, now_str, platform, category))
            return cur.rowcount

    def fix_all_updating_records(self):
        """紧急恢复：全局将 status=1 改为2"""
        now_ts = int(time.time())
        now_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        with self.conn.cursor() as cur:
            sql = """
            UPDATE pod_episode_ranking
            SET status=2,
                ranking_end_time=%s,
                updated_at=%s
            WHERE status=1
            """
            cur.execute(sql, (now_ts, now_str))
            fix_count = cur.rowcount
            if fix_count > 0:
                logger.warning(f"[MySQL] fix_all_updating_records: 修复{fix_count}条卡在status=1的记录")
            self.conn.commit()
        return fix_count

    def print_all_data(self):
        """
        打印当前数据库中所有记录(仅用于检查)
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM pod_episode_ranking ORDER BY id ASC")
            rows = cur.fetchall()
            logger.info("[MySQL] 当前 pod_episode_ranking 表数据如下:")
            for row in rows:
                logger.info(row)


# ------------------------
#   Apple & Spotify 综合爬虫示例
# ------------------------
class ApplePodcastScraper:
    """Apple & Spotify 播客爬虫示例（内含 ES 写入+两级匹配 + MySQL 写入）"""

    def __init__(self):
        # 1) 浏览器启动参数
        options = ChromiumOptions()
        options.set_argument('--remote-debugging-port=9222')  # 远程调试端口
        options.set_argument('--no-sandbox')                  # Linux 必需参数
        options.set_argument('--headless=new')                # 无头模式
        options.set_argument('--disable-gpu')
        options.set_argument('--disable-software-rasterizer')
        options.set_argument('--disable-dev-shm-usage')

        # Apple 部分（需要浏览器）
        self.driver = ChromiumPage(options)
        self.base_url = 'https://podcasts.apple.com/us/charts'
        self.itunes_api_url = "https://itunes.apple.com/lookup?id={}"

        # Spotify 部分（无需浏览器）
        self.spotify_api_url = "https://podcastcharts.byspotify.com/api/charts/top_episodes"
        self.spotify_items = 100  # 【第三修改：spotify 爬100条】

        # 外部 ES 查询相关(用于匹配 es_id)
        self.es_api_url = 'http://36.213.11.28:8089/podcast/search_by_dql'
        self.es_index = 'nex_basic_content_podcast_digest_chunk_emb'

        # HTTP Session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/131.0.0.0 Safari/537.36')
        })

        # 锁与缓存
        self.data_lock = Lock()
        self.cache_lock = Lock()

        # Apple / Spotify 通用
        self.problematic_rss_urls = set()  # 有问题的 RSS URL
        self.podcast_cache = {}           # 缓存已查过的 Apple ID 或搜索结果

        # Apple 每个分类爬取200条  【第三修改：每类200条】
        self.items_per_category = 200

        # 初始化 ElasticsearchHandler + MySQLHandler
        self.es_handler = ElasticsearchHandler()
        self.mysql_handler = MySQLHandler()

    # ------------------------
    #   通用 & 浏览器管理
    # ------------------------
    def initialize_browser(self):
        """初始化浏览器"""
        try:
            logger.info("浏览器已初始化")
            return True
        except Exception as e:
            logger.error(f"浏览器初始化失败: {e}")
            return False

    def cleanup(self):
        """关闭浏览器"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
        except Exception as e:
            logger.error(f"浏览器清理出错: {e}")

    # ------------------------
    #   Apple 爬虫相关
    # ------------------------
    def navigate_to_top_episodes(self):
        """点击进入Top Episodes页面"""
        try:
            self.driver.get(self.base_url)
            time.sleep(2)
            btn = self.driver.ele('xpath://button[contains(., "Top Episodes")]')
            if btn:
                btn.click()
                time.sleep(1)
                logger.info("成功导航到Top Episodes页面")
                return True
            else:
                logger.error("未找到Top Episodes按钮")
                return False
        except Exception as e:
            logger.error(f"导航失败: {e}")
            return False

    def switch_category(self, category_name):
        """切换到指定分类"""
        try:
            xpath = f'//select/option[contains(text(), "{category_name}")]'
            category_elem = self.driver.ele(f'xpath:{xpath}')
            if category_elem:
                category_elem.click()
                time.sleep(2)
                logger.info(f"成功切换到分类: {category_name}")
                return True
            else:
                logger.warning(f"未找到分类: {category_name}")
                return False
        except Exception as e:
            logger.error(f"切换分类失败: {e}")
            return False

    def scroll_page(self):
        """页面下拉，尝试加载更多内容"""
        try:
            js = '''
            const page = document.querySelector(".scrollable-page");
            if(page) {
                page.scrollTo(0, page.scrollHeight);
                return true;
            }
            return false;
            '''
            result = self.driver.run_js(js)
            if result:
                time.sleep(1.5)  # 增加等待时间以确保页面加载
            return bool(result)
        except Exception:
            return False

    def get_base_data(self, category_name, total_items=10):
        """
        在 Apple Top Episodes 列表页，通过下拉加载的方式
        获取指定分类下的基础数据
        """
        base_data = []
        scroll_count = 0
        no_new_data_count = 0
        previous_item_count = 0

        logger.info(f"开始获取分类 [{category_name}] 的数据，目标数量: {total_items}")

        # 计算理论上需要的滚动次数
        expected_scrolls = (total_items + 49) // 50
        # 【第三修改：滚动4次即可】
        max_scrolls = 4

        while len(base_data) < total_items and scroll_count < max_scrolls:
            try:
                items = self.driver.eles('css:.grid-item')
                current_item_count = len(items)

                if current_item_count <= previous_item_count:
                    no_new_data_count += 1
                else:
                    no_new_data_count = 0

                previous_item_count = current_item_count

                # 如果连续5次没有新数据且已达目标90%，则提前结束
                if no_new_data_count >= 5 and len(base_data) >= total_items * 0.9:
                    logger.info(f"已获取{len(base_data)}条数据，连续{no_new_data_count}次无新数据，提前结束")
                    break

                # 处理当前页面增量
                for item in items[len(base_data):]:
                    try:
                        rank_ele = item.ele('css:.episode-details__rank')
                        title_ele = item.ele('css:[data-testid="episode-lockup-title"]')
                        url_ele = item.ele('css:[data-testid="click-action"]')
                        if all([rank_ele, title_ele, url_ele]):
                            ep = {
                                'category': category_name,
                                'rank': rank_ele.text,
                                'episode': title_ele.text,
                                'url': url_ele.attr('href'),
                                'rss_url': None,
                                'audio_url': None,
                                'channel_name': None,
                                'es_id': None,
                            }
                            # 可选字段
                            date_ele = item.ele('css:.published-date')
                            if date_ele:
                                ep['publish_date'] = date_ele.text
                            duration_ele = item.ele('css:[data-testid="episode-duration"]')
                            if duration_ele:
                                ep['duration'] = duration_ele.text

                            base_data.append(ep)
                            if len(base_data) % 50 == 0:
                                logger.info(f"已获取 {len(base_data)} 条数据")

                    except Exception as e:
                        logger.error(f"处理单条数据出错: {e}")
                        continue

                    if len(base_data) >= total_items:
                        break

                if len(base_data) >= total_items:
                    break

                self.scroll_page()
                scroll_count += 1
                logger.info(f"已滚动 {scroll_count} 次，当前获取 {len(base_data)} 条数据")

            except Exception as e:
                logger.error(f"获取数据时出错: {e}")
                break

        logger.info(f"完成获取 [{category_name}] 分类数据，共 {len(base_data)} 条")
        return base_data[:total_items] if len(base_data) > total_items else base_data

    def get_podcast_info_from_apple(self, podcast_url):
        """调用Apple API获取播客的RSS链接和播客名称"""
        if not podcast_url:
            return None, None

        match = re.search(r'id(\d+)', podcast_url)
        if not match:
            # 如果URL中没有id，直接从URL中提取播客名称
            try:
                channel_name = podcast_url.split('/')[-1].replace('-', ' ').title()
                return None, channel_name
            except:
                return None, None

        podcast_id = match.group(1)
        with self.cache_lock:
            if podcast_id in self.podcast_cache:
                return self.podcast_cache[podcast_id]

        try:
            response = self.session.get(self.itunes_api_url.format(podcast_id), timeout=10)
            if response.status_code == 200:
                data = response.json()
                if "results" in data and len(data["results"]) > 0:
                    rss_url = data["results"][0].get("feedUrl", None)
                    channel_name = data["results"][0].get("collectionName", None)
                    with self.cache_lock:
                        self.podcast_cache[podcast_id] = (rss_url, channel_name)
                    return rss_url, channel_name

            elif response.status_code == 429:  # 速率限制
                logger.warning("API速率限制，等待2秒后重试...")
                time.sleep(2)
                response2 = self.session.get(self.itunes_api_url.format(podcast_id), timeout=10)
                if response2.status_code == 200:
                    data2 = response2.json()
                    if "results" in data2 and len(data2["results"]) > 0:
                        rss_url = data2["results"][0].get("feedUrl", None)
                        channel_name = data2["results"][0].get("collectionName", None)
                        with self.cache_lock:
                            self.podcast_cache[podcast_id] = (rss_url, channel_name)
                        return rss_url, channel_name

        except requests.exceptions.Timeout:
            logger.warning(f"获取podcast信息超时: {podcast_id}")
        except Exception as e:
            logger.error(f"获取podcast信息出错: {e}")

        # 如果无法获取, 尝试从URL末尾分割出名称
        try:
            c_name = podcast_url.split('/')[-1].replace('-', ' ').title()
            with self.cache_lock:
                self.podcast_cache[podcast_id] = (None, c_name)
            return None, c_name
        except:
            pass

        return None, None

    def get_audio_url_from_rss(self, rss_url):
        """解析RSS获取音频链接"""
        if not rss_url:
            return None
        if rss_url in self.problematic_rss_urls:
            logger.info(f"跳过已知有问题的RSS URL: {rss_url}")
            return None

        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(3.0)
        try:
            # 使用 feedparser 解析
            try:
                feed = feedparser.parse(rss_url, request_headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                    'Accept': 'application/rss+xml, application/xml, text/xml'
                })
            except Exception as e:
                logger.warning(f"RSS解析失败: {e}, RSS_URL: {rss_url}")
                self.problematic_rss_urls.add(rss_url)
                return None

            if not feed or not hasattr(feed, 'entries') or len(feed.entries) == 0:
                logger.warning(f"RSS解析结果为空: {rss_url}")
                self.problematic_rss_urls.add(rss_url)
                return None

            for entry in feed.entries[:1]:  # 只取最新一期
                if hasattr(entry, 'enclosures') and entry.enclosures:
                    return entry.enclosures[0].get('href', None)
                if hasattr(entry, 'links'):
                    for link in entry.links:
                        if link.get('type', '').startswith('audio/'):
                            return link.get('href')
                if hasattr(entry, 'media_content'):
                    for media in entry.media_content:
                        if isinstance(media, dict) and 'url' in media:
                            return media.get('url')
                break
        except Exception as e:
            logger.error(f"解析RSS时出错: {e}, RSS_URL: {rss_url}")
            self.problematic_rss_urls.add(rss_url)
        finally:
            socket.setdefaulttimeout(old_timeout)
        return None

    def process_apple_item(self, item):
        """多线程：为 Apple 数据补充 RSS、音频URL、频道名称"""
        try:
            title = item.get('episode', item.get('title', '未知标题'))
            logger.info(f"[Apple] 正在处理条目: {title}")

            rss_url, channel_name = self.get_podcast_info_from_apple(item.get('url', ''))
            audio_url = None
            if rss_url:
                audio_url = self.get_audio_url_from_rss(rss_url)

            item['rss_url'] = rss_url
            item['audio_url'] = audio_url
            item['channel_name'] = channel_name

            logger.info(f"[Apple] 处理完成: {title} | channel={channel_name}")
        except Exception as e:
            logger.error(f"处理 Apple 数据时出错: {e}")
        return item

    # ------------------------
    #   Spotify 爬虫相关
    # ------------------------
    def get_real_spotify_data(self, region='us', limit=10):
        """通过 Spotify API 获取排行榜数据，只取前 limit 条"""
        logger.info(f"开始请求 Spotify API: region={region}, limit={limit}")
        try:
            params = {'region': region}
            resp = self.session.get(self.spotify_api_url, params=params, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Spotify API 请求失败，status_code={resp.status_code}")
                return []

            data = resp.json()
            if not isinstance(data, list):
                logger.error("Spotify API 返回的数据结构不符合预期（不是list）")
                return []

            data = data[:limit]
            results = []
            for idx, item in enumerate(data, start=1):
                ep_name = item.get('episodeName', f"NoTitle_{idx}")
                show_name = item.get('showName', "Unknown Channel")
                rank = item.get('position', idx) + 1  # position 可能从0开始
                ep_uri = item.get('episodeUri', '')
                ep_url = None
                if ep_uri.startswith("spotify:episode:"):
                    ep_id = ep_uri.split(':')[-1]
                    ep_url = f"https://open.spotify.com/episode/{ep_id}"

                results.append({
                    'category': 'SpotifyTopEpisodes',
                    'rank': rank,
                    'episode': ep_name,
                    'url': ep_url,
                    'rss_url': None,
                    'audio_url': None,
                    'channel_name': show_name,
                    'es_id': None,
                })
            return results

        except Exception as e:
            logger.error(f"获取Spotify数据时出错: {e}", exc_info=True)
            return []

    def get_spotify_data(self, total_items=10):
        """获取 Spotify 榜单"""
        region = 'us'
        data_list = self.get_real_spotify_data(region=region, limit=total_items)
        logger.info(f"Spotify API 返回 {len(data_list)} 条。")
        return data_list

    def process_spotify_item(self, item):
        """
        多线程：为 Spotify 条目补充 RSS/音频链接
        这里示例：通过 showName -> 在 Apple 搜索 同名播客 -> 拿到 RSS -> 解析音频
        """
        try:
            title = item.get('episode', '未知标题')
            channel_name = item.get('channel_name', '')
            logger.info(f"[Spotify] 正在处理条目: {title}")

            rss_url = None
            if channel_name:
                rss_url, _ = self.search_apple_by_channel(channel_name)

            audio_url = None
            if rss_url:
                audio_url = self.get_audio_url_from_rss(rss_url)

            item['rss_url'] = rss_url
            item['audio_url'] = audio_url

            logger.info(f"[Spotify] 处理完成: {title} | rss={rss_url}")
        except Exception as e:
            logger.error(f"[Spotify] 处理数据时出错: {e}")
        return item

    def search_apple_by_channel(self, channel_name):
        """
        给定 channel_name(播客名称), 调用 Apple Podcast 搜索接口
        获取 rss_url 与 频道名(同传入即可)
        """
        if not channel_name:
            return None, channel_name

        with self.cache_lock:
            if channel_name in self.podcast_cache:
                return self.podcast_cache[channel_name]

        try:
            encoded_name = urllib.parse.quote(channel_name)
            search_url = f"https://itunes.apple.com/search?term={encoded_name}&media=podcast&limit=1"
            response = self.session.get(search_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and "results" in data and len(data["results"]) > 0:
                    rss_url = data["results"][0].get("feedUrl", "")
                    with self.cache_lock:
                        self.podcast_cache[channel_name] = (rss_url, channel_name)
                    return rss_url, channel_name
        except Exception as e:
            logger.error(f"Apple搜索频道失败: {channel_name}, 错误: {e}")

        with self.cache_lock:
            self.podcast_cache[channel_name] = (None, channel_name)
        return None, channel_name

    # ------------------------
    #   多线程处理
    # ------------------------
    def enrich_data_multithreaded(self, data, max_workers=10, is_spotify=False):
        """多线程为数据补充RSS、音频URL、频道名称等"""
        # 【第二修改：改为20线程处理】
        max_workers = 20

        logger.info(f"开始多线程处理 {len(data)} 条{'Spotify' if is_spotify else 'Apple'} 数据...")

        processed_data = []
        batch_size = 10
        func = self.process_spotify_item if is_spotify else self.process_apple_item

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                futures = {executor.submit(func, item): item for item in batch}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        item = future.result(timeout=15)
                        processed_data.append(item)
                    except concurrent.futures.TimeoutError:
                        logger.error("处理任务超时，跳过当前任务")
                        original_item = futures[future]
                        processed_data.append(original_item)
                    except Exception as e:
                        logger.error(f"获取处理结果时出错: {e}")
                        original_item = futures[future]
                        processed_data.append(original_item)
                time.sleep(0.2)

        logger.info(f"多线程处理完成，共处理 {len(processed_data)} 条数据")
        return processed_data

    # ------------------------
    #   ES 匹配逻辑
    # ------------------------
    def search_es_by_title_and_rss(self, title, rss_url):
        """根据 (title + rss_url) 搜索外部索引"""
        if not title or not rss_url:
            return None
        try:
            dql = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"title": {"query": title, "operator": "and"}}},
                            {"match": {"rss_url": {"query": rss_url, "operator": "and"}}}
                        ]
                    }
                },
                "_source": ["id", "title", "rss_url"],
                "size": 1
            }
            data = {"dql": json.dumps(dql), "index": self.es_index}
            response = self.session.post(self.es_api_url, json=data, timeout=10)
            if response.status_code == 200:
                rj = response.json()
                if rj and 'data' in rj and len(rj['data']) > 0:
                    return rj['data'][0]['id']
            return None
        except Exception as e:
            logger.error(f"(标题+RSS)搜索ES出错: {e}")
            return None

    def search_es_by_title_and_channel(self, title, channel_name):
        """根据 (title + channel_name) 搜索外部索引"""
        if not title or not channel_name:
            return None
        try:
            dql = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"title": {"query": title, "operator": "and"}}},
                            {"match": {"channel_name": {"query": channel_name, "operator": "and"}}}
                        ]
                    }
                },
                "_source": ["id", "title", "channel_name"],
                "size": 1
            }
            data = {"dql": json.dumps(dql), "index": self.es_index}
            response = self.session.post(self.es_api_url, json=data, timeout=10)
            if response.status_code == 200:
                rj = response.json()
                if rj and 'data' in rj and len(rj['data']) > 0:
                    return rj['data'][0]['id']
            return None
        except Exception as e:
            logger.error(f"(标题+频道)搜索ES出错: {e}")
            return None

    def match_with_es(self, data):
        """
        与外部索引做两级匹配:
          1) (title + rss_url)
          2) (title + channel_name)
        若都没匹配到 -> es_id = None
        """
        logger.info(f"开始与ES数据库匹配，共 {len(data)} 条数据...")
        matched_count = 0
        for item in data:
            item['es_id'] = None

            title = item.get('episode')
            rss_url = item.get('rss_url')
            channel_name = item.get('channel_name')

            # 第一级
            esid = None
            if title and rss_url:
                esid = self.search_es_by_title_and_rss(title, rss_url)
                if esid:
                    matched_count += 1
                    item['es_id'] = esid
                    logger.info(f"[ES匹配] (标题+RSS) 成功 -> {title}, es_id={esid}")
                else:
                    # 第二级
                    if title and channel_name:
                        esid2 = self.search_es_by_title_and_channel(title, channel_name)
                        if esid2:
                            matched_count += 1
                            item['es_id'] = esid2
                            logger.info(f"[ES匹配] (标题+频道) 成功 -> {title}, es_id={esid2}")
            else:
                # 不满足第一级条件 -> 尝试第二级
                if title and channel_name:
                    esid2 = self.search_es_by_title_and_channel(title, channel_name)
                    if esid2:
                        matched_count += 1
                        item['es_id'] = esid2
                        logger.info(f"[ES匹配] (标题+频道) 成功 -> {title}, es_id={esid2}")

            logger.info(f"[ES匹配] 条目: {title}, 最终 es_id={item['es_id']}")

        logger.info(f"ES匹配完成，共匹配到 {matched_count}/{len(data)} 条")
        return data

    # ------------------------
    #   统一执行入口
    # ------------------------
    def run(self):
        """
        1) 初始化浏览器 & 进入 Apple
        2) 抓取 Apple 所有分类(每类 200 条) -> 多线程补充 -> 两级ES匹配 -> 入 episode_ranking
        3) 抓取 Spotify 100 条 -> 多线程补充 -> 两级ES匹配 -> 入 episode_ranking
        4) 返回所有数据
        5) 同步入 MySQL + 打印 MySQL 数据做检查
        """
        all_data = []
        try:
            if not self.initialize_browser():
                return all_data

            # 先进入 Apple 的 Top Episodes
            if not self.navigate_to_top_episodes():
                return all_data

            # =========== (A) Apple 所有分类 ===========
            cat_list = self.driver.eles('xpath://select/option')
            categories = [ele.text for ele in cat_list]
            logger.info(f"Apple 分类列表: {categories}")

            # 分类
            categories_to_process = categories  # 不排除任何分类
            logger.info(f"将处理所有 Apple 分类: {categories_to_process}")

            apple_all = []
            for category_name in categories_to_process:
                logger.info(f"\n[Apple] 处理分类: {category_name}")

                # 修改 "All Categories" 为 "Apple Top"
                if category_name == "All Categories":
                    category_name = "Apple Top"

                if category_name != "Apple Top":
                    if not self.switch_category(category_name):
                        continue

                base_data = self.get_base_data(category_name, self.items_per_category)
                if not base_data:
                    continue

                # 多线程补充
                enriched = self.enrich_data_multithreaded(base_data, max_workers=10, is_spotify=False)
                # ES 匹配
                final_data = self.match_with_es(enriched)

                # 写入 ES
                self.es_handler.update_es(final_data, chart_type=category_name, explicit_platform="apple")
                # 同步写入 MySQL
                self.mysql_handler.update_mysql(final_data, chart_type=category_name, explicit_platform="apple")

                apple_all.extend(final_data)

            logger.info(f"[Apple] 分类处理完成，共获取 {len(apple_all)} 条数据")

            # =========== (B) Spotify 100 条 ===========
            spotify_base = self.get_spotify_data(total_items=self.spotify_items)
            if spotify_base:
                # 多线程补充
                spotify_enriched = self.enrich_data_multithreaded(spotify_base, max_workers=10, is_spotify=True)
                # ES 匹配
                spotify_final = self.match_with_es(spotify_enriched)

                # 入 ES
                self.es_handler.update_es(spotify_final, chart_type="spotify_top", explicit_platform="spotify")
                # 入 MySQL
                self.mysql_handler.update_mysql(spotify_final, chart_type="spotify_top", explicit_platform="spotify")
            else:
                logger.warning("[Spotify] 未获取到任何数据")
                spotify_final = []

            # 合并
            all_data = apple_all + spotify_final
            logger.info(f"全部数据处理完成，总计 {len(all_data)} 条")

        except Exception as e:
            logger.error(f"执行发生错误: {e}", exc_info=True)
            self.es_handler.fix_all_updating_records()
            self.mysql_handler.fix_all_updating_records()
        finally:
            self.cleanup()

        return all_data


# ------------------------
#   主运行 (打印结果)
# ------------------------
if __name__ == "__main__":
    scraper = ApplePodcastScraper()
    data = scraper.run()

    logger.info(f"\n===== 最终总共获取 {len(data)} 条数据，下面打印部分结果供检查 =====")
    print(json.dumps(data[:10], ensure_ascii=False, indent=2))  # 只打印前10条做示例

    logger.info("\n===== 同步到 MySQL 后, 打印 MySQL 当前所有数据供检查 =====")
    scraper.mysql_handler.print_all_data()

    logger.info("===== 爬虫执行完毕 =====")

    # ------------------------ 【新增：飞书报告】 ------------------------
    # 1) 统计 es_id 为空的条数
    esid_none_count = sum(1 for d in data if not d.get('es_id'))

    # 2) 随机打印 ES 库里 5 条的 (es_id, episode_id)，以及 MySQL 里 5 条的 (episode_id)
    #    - ES 查询
    try:
        es = scraper.es_handler.es
        index_name = scraper.es_handler.index_name
        es_resp = es.search(index=index_name, body={"query": {"match_all": {}}, "size": 500})
        hits = es_resp["hits"]["hits"]
        es_random_5 = []
        if hits:
            selected = random.sample(hits, min(5, len(hits)))
            for h in selected:
                _id = h["_id"]
                ep_id = h["_source"].get("episode_id", "")
                es_random_5.append(f"doc_id={_id}, episode_id={ep_id}")
        else:
            es_random_5.append("无数据")

    except Exception as e:
        es_random_5 = [f"查询异常: {e}"]

    #    - MySQL 查询
    try:
        with scraper.mysql_handler.conn.cursor() as cur:
            cur.execute("SELECT episode_id FROM pod_episode_ranking ORDER BY RAND() LIMIT 5")
            rows = cur.fetchall()
            mysql_random_5 = [f"episode_id={r['episode_id']}" for r in rows]
            if not mysql_random_5:
                mysql_random_5 = ["无数据"]
    except Exception as e:
        mysql_random_5 = [f"查询异常: {e}"]

    # 3) 构造报告
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_count = len(data)

    # 由于代码2并未统计 Apple/Spotify 的新上榜/下榜等详细数字，这里简单统计 Apple/Spotify 区分(仅爬取条数)
    apple_count = sum(1 for d in data if d.get('category') != 'SpotifyTopEpisodes')
    spotify_count = sum(1 for d in data if d.get('category') == 'SpotifyTopEpisodes')

    report_lines = [
        f"时间：{now_str}",
        f"====================",
        f"Apple本次爬取：{apple_count} 条；",
        f" - 新上榜：0 (代码2未统计，可自行扩展)",
        f" - 重新上榜：0 (代码2未统计，可自行扩展)",
        f" - 下榜：0 (代码2未统计，可自行扩展)",
        f" - 删除(下榜72小时+)：0 (代码2未统计，可自行扩展)",
        f"--------------------",
        f"Spotify本次爬取：{spotify_count} 条；",
        f" - 新上榜：0 (代码2未统计，可自行扩展)",
        f" - 重新上榜：0 (代码2未统计，可自行扩展)",
        f" - 下榜：0 (代码2未统计，可自行扩展)",
        f" - 删除(下榜72小时+)：0 (代码2未统计，可自行扩展)",
        f"====================",
        f"总计爬取：{total_count} 条。",
        f"其中 es_id 为空的有：{esid_none_count} 条",
        f"--------------------",
        f"【随机打印 ES 中 5 条】:",
        *[f"   {x}" for x in es_random_5],
        f"--------------------",
        f"【随机打印 MySQL 中 5 条】:",
        *[f"   {x}" for x in mysql_random_5],
        f"===================="
    ]
    final_report = "\n".join(report_lines)
    logger.info(f"[Main] 飞书报告:\n{final_report}")

    # 4) 发送到飞书
    send_feishu_report(final_report)
    logger.info("===== 报告发送完毕 =====")

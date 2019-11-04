#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2019/10/29 13:30
# @Author  : Xtmin
# @Email   : wangdaomin123@hotmail.com
# @File    : ht_exam.py
# @Software: PyCharm
import sys
import json
import jpush
import random
import asyncio
import aiohttp
import logging
import datetime
import aiosqlite3
from lxml import etree
from dotenv import dotenv_values
from dateutil.parser import parse as dp
from apscheduler.schedulers.asyncio import AsyncIOScheduler

j_push_config = dotenv_values('push.env')

log_path = j_push_config.get('log_path')
app_key = j_push_config.get('app_key')
master_secret = j_push_config.get('master_secret')

if not app_key or not master_secret or not log_path:
    sys.exit('配置文件异常，无法获取推送配置')

logger = logging.getLogger(__name__)
format_pattern = '[%(asctime)s %(levelname)s] %(filename)s:%(lineno)s %(message)s'
log_formatter = logging.Formatter(format_pattern)
logging.basicConfig(level=logging.INFO, format=format_pattern)
file_handler = logging.FileHandler(j_push_config.get('log_path'), encoding='utf8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

host = 'http://m.sc.huatu.com'
base_url = 'http://m.sc.huatu.com/index/Articlelist/getMoreArt/'

key_word_list = [
    '四川省', '成都', '金堂', '青白江', '都江堰', '大邑', '彭州', '崇州'
]


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


encoder = JsonEncoder()
encoder.ensure_ascii = False

ua_list = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/72.0.3626.101 Mobile/15E148 Safari/605.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/15.0b13894 Mobile/16D57 Safari/605.1.15',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/8.1.1 Mobile/16D57 Safari/605.1.15',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) OPiOS/16.0.14.122053 Mobile/16D57 Safari/9537.53',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) OPT/2 Mobile/16D57',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) OPiOS/12.0.5.3 Version/7.0 Mobile/16D57 Safari/9537.53',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 EdgiOS/42.10.3 Mobile/16D57 Safari/605.1.15',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16D57 unknown BingWeb/6.9.8.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 MQQBrowser/9.0.3 Mobile/16D57 Safari/604.1 MttCustomUA/2 QBWebViewType/1 WKType/1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16D57 SearchCraft/3.4.1 (Baidu; P2 12.1.4)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/16D57 UCBrowser/12.3.0.1138 Mobile AliApp(TUnionSDK/0.1.20.3)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X; zh-cn) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/16D57 Quark/3.0.6.926 Mobile',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16D57 MicroMessenger/7.0.3(0x17000321) NetType/WIFI Language/zh_CN',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16A366 QQ/7.8.8.420 V1_IPH_SQ_7.8.8_1_APP_A Pixel/1125 Core/WKWebView Device/Apple(iPhone X) NetType/4G QBWebViewType/1 WKType/1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12. Mobile/16D57 Safari/600.1.4 baidubrowser/4.14.1.11 (Baidu; P2 12.1.4)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16D57 baiduboxapp/11.3.6.10 (Baidu; P2 12.1.4)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/606.4.5 (KHTML, like Gecko) Mobile/16D57 QHBrowser/317 QihooBrowser/4.0.10',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16D57',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16D57 Mb2345Browser/5.2.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/10.0 Mobile/16D57 Safari/602.1 MXiOS/5.2.20.508',
]

_jpush = jpush.JPush(app_key, master_secret)
push = _jpush.create_push()
push.audience = jpush.all_

push.platform = jpush.all_


def __dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


insert_sql = 'INSERT INTO huatu (exam_id, exam_title, exam_url, pub_time, status, exam_type) VALUES (?, ?, ?, ?, ?, ?)'
update_sql = 'UPDATE huatu SET exam_title=?, exam_url=?, update_time=current_timestamp, status=? WHERE exam_id=?'


async def init_db(loop):
    conn = await aiosqlite3.connect('./huatu.db', loop=loop, echo=True)
    conn.row_factory = __dict_factory
    cursor = await conn.cursor()
    create_sql = """
            CREATE TABLE if NOT EXISTS huatu (id INTEGER PRIMARY KEY NOT NULL, exam_id INTEGER NOT NULL UNIQUE, exam_title CHAR (128), exam_url CHAR (128), pub_time TIMESTAMP, update_time TIMESTAMP DEFAULT (datetime('now', 'localtime')), build_time TIMESTAMP DEFAULT (datetime('now', 'localtime')), status CHAR (8), exam_type CHAR (8))
                """
    await cursor.execute(create_sql)
    logger.info('init_db finish')
    return conn, cursor


async def close_db(conn, cursor):
    await cursor.close()
    await conn.close()
    logger.info('close database')


async def fetch(session, url):
    async with session.get(url) as resp:
        resp_json = await resp.json(content_type='text/html', encoding='utf-8')
        return resp_json


async def fetch_teacher_exam_first_page(session, url):
    async with session.get(url) as resp:
        resp_text = await resp.text()
        return resp_text


async def crawl_teacher_exam(conn, cursor):
    logger.debug('enter crawl_teacher_exam')
    # 获取教师公招招聘
    async with aiohttp.ClientSession(headers={'User-Agent': random.choice(ua_list)}) as session:
        resp_text = await fetch_teacher_exam_first_page(session, 'http://m.sc.huatu.com/list/jiaoshi/kaoshi/')
        tree = etree.HTML(resp_text)
        tids = tree.xpath('//input[@name="tids"]/@value')[0]
        i_lst = []
        u_lst = []
        for _tree_elem in tree.xpath('//ul[@class="list"]/li'):
            _url = _tree_elem.xpath('./a/@href')[0]
            _id = _url[_url.rindex('/') + 1:_url.rindex('.html')]
            title = _tree_elem.xpath('.//p[@class="item-tit"]/text()')[0]
            c_flag = False
            for key in key_word_list:
                if key in title:
                    c_flag = True
                    break
            if not c_flag:
                continue
            time = _tree_elem.xpath('.//p[@class="item-time"]/text()')[0]
            result = await data_exist(cursor, _id)
            if not result:
                i_lst.append((_id, title, f'{host}{_url}', dp(time), 'pre_notice', 'teacher'))
            else:
                if title != result.get('title'):
                    u_lst.append((title, f'{host}{_url}', dp(time), _id, 'update_notice'))
        for index in range(2, 4):
            resp = await fetch(session, f'{base_url}?page={index}&tids={tids}')
            for item in (resp.get('data') or []):
                if item and 'id' in item and 'title' in item and 'date' in item and 'url' in item:
                    c_flag = False
                    for key in key_word_list:
                        if key in item.get('title'):
                            c_flag = True
                            break
                    if not c_flag:
                        continue
                    result = await data_exist(cursor, _id)
                    if not result:
                        i_lst.append((item.get('id'), item.get('title'), f"{host}{item.get('url')}", dp(item.get('date')), 'pre_notice', 'teacher'))
                    else:
                        if title != result.get('title'):
                            u_lst.append((title, f'{host}{_url}', dp(time), _id, 'update_notice'))
        logger.debug('crawl teacher exam finish')
        logger.info(f"add teacher data {json.dumps(i_lst, ensure_ascii=False)}")
        logger.info(f"update teacher data {json.dumps(i_lst, ensure_ascii=False)}")
        if i_lst:
            await cursor.executemany(insert_sql, tuple(i_lst))
        if u_lst:
            await cursor.executemany(update_sql, tuple(u_lst))
        await conn.commit()


async def crawl_government_exam(conn, cursor):
    logger.debug('enter crawl_government_exam')
    # 获取事业单位招聘
    async with aiohttp.ClientSession(headers={'User-Agent': random.choice(ua_list)}) as session:
        resp_text = await fetch_teacher_exam_first_page(session, 'http://m.sc.huatu.com/list/sydw/kaoshi/')
        tree = etree.HTML(resp_text)
        tids = tree.xpath('//input[@name="tids"]/@value')[0]
        i_lst = []
        u_lst = []
        for _tree_elem in tree.xpath('//ul[@class="list"]/li'):
            _url = _tree_elem.xpath('./a/@href')[0]
            _id = _url[_url.rindex('/') + 1:_url.rindex('.html')]
            title = _tree_elem.xpath('.//p[@class="item-tit"]/text()')[0]
            c_flag = False
            for key in key_word_list:
                if key in title:
                    c_flag = True
                    break
            if not c_flag:
                continue
            time = _tree_elem.xpath('.//p[@class="item-time"]/text()')[0]
            result = await data_exist(cursor, _id)
            if not result:
                i_lst.append((_id, title, f'{host}{_url}', dp(time), 'pre_notice', 'gov'))
            else:
                if title != result.get('title'):
                    u_lst.append((title, f'{host}{_url}', dp(time), _id, 'update_notice'))
        for index in range(2, 4):
            resp = await fetch(session, f'{base_url}?page={index}&tids={tids}')
            for item in (resp.get('data') or []):
                if item and 'id' in item and 'title' in item and 'date' in item and 'url' in item:
                    c_flag = False
                    for key in key_word_list:
                        if key in item.get('title'):
                            c_flag = True
                            break
                    if not c_flag:
                        continue
                    result = await data_exist(cursor, _id)
                    if not result:
                        i_lst.append((item.get('id'), item.get('title'), f"{host}{item.get('url')}", dp(item.get('date')), 'pre_notice', 'gov'))
                    else:
                        if title != result.get('title'):
                            u_lst.append((title, f'{host}{_url}', dp(time), _id, 'update_notice'))
        logger.debug('crawl government exam finish')
        logger.info(f"add teacher data {json.dumps(i_lst, ensure_ascii=False)}")
        logger.info(f"update teacher data {json.dumps(i_lst, ensure_ascii=False)}")
        if i_lst:
            await cursor.executemany(insert_sql, tuple(i_lst))
        if u_lst:
            await cursor.executemany(update_sql, tuple(u_lst))
        await conn.commit()


async def data_exist(cursor, _id):
    await cursor.execute('SELECT exam_title FROM huatu WHERE exam_id=? limit 1', (_id,))
    result = await cursor.fetchone()
    return result


async def push_notice(conn, cursor):
    def j_push(exam_info):
        try:
            push.notification = jpush.notification(
                android={
                    'title': f"新的{'事业单位' if exam_info.get('exam_type') == 'gov' else '教师公招'}公告",
                    'alert': f'''
                        地址：{exam_info.get('exam_url')}
                        标题：{exam_info.get('exam_title')}
                        '''
                }
            )
            push.send()
        except jpush.common.Unauthorized:
            logger.error('Unauthorized')
        except jpush.common.APIConnectionException:
            logger.error('conn error')
        except jpush.common.JPushFailure:
            logger.error("JPushFailure")
        except:
            logger.error("Exception")

    await cursor.execute("SELECT exam_title, exam_url, pub_time, exam_type FROM huatu WHERE status='pre_notice'")
    result = await cursor.fetchall()
    for item in result:
        j_push(item)
        await cursor.execute("UPDATE huatu SET exam_type='noticed', update_time=CURRENT_TIMESTAMP WHERE exam_id=?", item.get('exam_id'))
        await conn.commit()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    conn, cursor = loop.run_until_complete(init_db(loop))
    sched = AsyncIOScheduler(event_loop=loop)
    sched.add_job(crawl_teacher_exam, args=[conn, cursor], trigger='interval', minutes=30)
    sched.add_job(crawl_government_exam, args=[conn, cursor], trigger='interval', minutes=30)
    sched.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(close_db(conn, cursor))
        logger.info('Shutting Down!')

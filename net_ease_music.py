#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2019/4/9 14:41
# @Author  : Xtmin
# @Email   : wangdaomin123@hotmail.com
# @File    : net_ease_music.py
# @Software: PyCharm
import re
import json
import time
import math
import base64
import random
import codecs
import asyncio
import sqlite3
import requests
import functools
from lxml import etree
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from string import ascii_letters, digits


class NetEaseMusic:
    def __init__(self):
        self.AES_KEY = None
        self.RES_KEY = None
        self.RES_MODULE = None
        self.home_page_url = 'https://music.163.com/'  # 首页链接，用来提取包含有加密参数的js地址
        self.encrypt_param_js_url = ''
        self.search_url = 'https://music.163.com/weapi/cloudsearch/get/web?csrf_token='  # 搜索链接
        self.song_detail_url = 'https://music.163.com/weapi/v1/resource/comments/R_SO_4_{}?csrf_token='  # 歌曲详情url
        self.charset = ascii_letters + digits
        self.limit = 30
        self.search_data = {
            "hlpretag": "<span class=\"s-fc7\">",
            "hlposttag": "</span>",
            "s": "婚礼",
            "type": "1",
            "offset": "0",
            "total": "true",
            "limit": f"{self.limit}",
            "csrf_token": ""
        }
        self.song_detail_data = {
            'rid': "R_SO_4_541687281",
            'offset': "0",
            'total': "true",
            'limit': "20",
            'csrf_token': ""
        }
        self.headers = {
            'Host': 'music.163.com',
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0',
            'Accept': '*/*',
            'Accept-Language':
                'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://music.163.com/search/',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Content-Length': '576',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }
        self.session = requests.Session()
        self.SQL_FILE = './netease.db'
        self.conn, self.cursor = None, None  # 数据库的连接和游标
        self.__init__db___()  # 初始化数据库连接
        self.loop = asyncio.get_event_loop()
        self.pre_request()  # 调用一次该方法，用来确定加密参数等需要的参数

    @staticmethod
    def __dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__db___(self):
        """
        这里为初始化数据库方法，我这里暂时使用sqlite3
        """
        self.conn = sqlite3.connect(self.SQL_FILE)
        self.conn.row_factory = self.__dict_factory
        self.cursor = self.conn.cursor()
        create_sql = """
        create table if not exists song_list (id integer primary key not null,name text not null, song_id int not null, song_info text not null)
                    """
        self.cursor.execute(create_sql)
        create_sql = """
        create table if not exists song_detail (id integer primary key not null,song_id int not null, comment_num int not null, song_info text not null)
                    """
        self.cursor.execute(create_sql)
        self.conn.commit()

    def close_db_connection(self):
        self.cursor.close()
        self.conn.close()

    def pre_request(self):
        print('enter pre_request')
        resp = requests.get(self.home_page_url)
        if resp.status_code != 200:
            pass
        tree = etree.HTML(resp.text)
        print('start parse html')
        _src = tree.xpath(
            "//script[contains(@src, 's3.music.126.net/web/s/core_')]/@src")
        if not _src:
            pass
        self.encrypt_param_js_url = f'https:{_src[0]}'
        resp = requests.get(
            self.encrypt_param_js_url,
            headers={'User-Agent': self.headers.get('User-Agent')})
        resp_body = resp.text
        _str = re.findall(r'window.asrsea\(JSON.stringify.*\)\);', resp_body)
        _md = re.findall(r'QI.{2}.md=\[.*\]', resp_body)
        _emj = re.findall('QI.{2}.emj={.*};', resp_body)
        if not _str or not _md or not _emj:
            return
        _str, _md, _emj = _str[0], _md[0], _emj[0]
        asr_sea_param = _str[_str.index('(') + 1:_str.rindex(')') if ')' in
                                                                     _str else -1].split('),')
        _rsa_key_gen_list = json.loads(
            asr_sea_param[1][asr_sea_param[1].index('(') +
                             1:asr_sea_param[1].rindex(')') if ')' in
                                                               asr_sea_param[1] else None])
        _aes_key_gen_list = json.loads(
            asr_sea_param[3][asr_sea_param[3].index('(') +
                             1:asr_sea_param[3].rindex(')') if ')' in
                                                               asr_sea_param[3] else None])
        md = json.loads(_md[8:])
        emj = json.loads(_emj[9:-1])
        print(f"res_key: {''.join([emj[_] for _ in _rsa_key_gen_list])}")
        print(f"RES_MODULE: {''.join([emj[_] for _ in md])}")
        print(f"AES_KEY: {''.join([emj[_] for _ in _aes_key_gen_list])}")
        self.RES_KEY = ''.join([emj[_] for _ in _rsa_key_gen_list])
        self.RES_MODULE = ''.join([emj[_] for _ in md])
        self.AES_KEY = ''.join([emj[_] for _ in _aes_key_gen_list])

    def rand_char(self, num=16):
        return ''.join(''.join(random.sample(self.charset, num)))

    @staticmethod
    def aes_encrypt(msg, key, iv=b'0102030405060708'):
        if isinstance(msg, str):
            msg = msg.encode('utf8')
        if isinstance(key, str):
            key = key.encode('utf8')
        encryptor = AES.new(key, IV=iv, mode=AES.MODE_CBC)
        text = encryptor.encrypt(pad(msg, AES.block_size))
        text = base64.b64encode(text)
        return text

    def gen_params(self, param, rand_key):
        if isinstance(param, dict):
            param = json.dumps(param,
                               ensure_ascii=False,
                               separators=(',', ':'))
        text = self.aes_encrypt(param, self.AES_KEY)
        text = self.aes_encrypt(text, rand_key)
        return text

    def gen_enc_sec_key(self, msg):
        sec_key = int(
            codecs.encode(msg[::-1].encode('utf8'), encoding='hex_codec'),
            16) ** int(self.RES_KEY, 16) % int(self.RES_MODULE, 16)
        encrypted_str = format(sec_key, 'x').zfill(256)
        return encrypted_str

    def crawl_music_list_with_key_word(self, key_word=None):
        """
        根据关键字获取歌单
        :param key_word:
        :return:
        """
        total_page = self.get_total_page()
        tasks = []
        for index in range(1, total_page + 1):
            tasks.append(self.get_data_by_page_key_word(index, key_word))
        song_list = self.loop.run_until_complete(asyncio.wait(tasks))
        for index, _ in enumerate(song_list[0]):
            self.store_song_list(_.result())
            print(f'crawl page {index} finish')

    def get_total_page(self):
        """
        先发送一次请求，获取到总共有多少页
        """
        rand_key = self.rand_char()
        post_data = {
            'params': self.gen_params(self.search_data, rand_key),
            'encSecKey': self.gen_enc_sec_key(rand_key)
        }
        resp = self.session.post(self.search_url,
                                 data=post_data,
                                 headers=self.headers)
        if resp.status_code == 200:
            resp_body = resp.json()
            total_page = math.ceil(
                resp_body.get('result', {}).get('songCount', 0) / self.limit)
            return total_page
        else:
            return 0

    async def get_data_by_page_key_word(self, page_num, key_word):
        """
        根据指定的页码和关键字来获取数据
        :param page_num: 需要获取数据的页码数
        :param key_word: 搜索关键字
        :return 请求获取到的页面json数据
        """
        rand_key = self.rand_char()
        self.search_data['s'] = key_word
        self.search_data['offset'] = str((page_num - 1) * self.limit)
        post_data = {
            'params': self.gen_params(self.search_data, rand_key),
            'encSecKey': self.gen_enc_sec_key(rand_key)
        }
        resp = await self.loop.run_in_executor(None, functools.partial(self.session.post, url=self.search_url, data=post_data, headers=self.headers))
        if resp.status_code == 200:
            resp_body = resp.json()
            total_data = resp_body.get('result', {}).get('songs', [])
            return total_data
        else:
            return []

    def store_song_list(self, song_list):
        """
        入库歌单里面的数据
        :param song_list: 歌单列表数据
        """
        insert_sql = '''replace into song_list (name, song_id, song_info) values (?, ?, ?)'''
        for item in song_list:
            self.cursor.execute(
                insert_sql,
                (item.get('name', ''), item.get('id', ''),
                 json.dumps(item, ensure_ascii=False, separators=(',', ':'))))
            print(f"insert song {item.get('name')}, id {item.get('id', '')}")
        self.conn.commit()
        print('commit')

    def crawl_song_list_from_sql(self):
        self.cursor.execute("select * from song_list")
        song_list = self.cursor.fetchall()
        tasks = []
        total_slice_num = math.ceil(len(song_list) / 10)
        for song in song_list:
            tasks.append(self.crawl_song_detail(song.get('song_id')))
        for index in range(total_slice_num):
            _tasks = tasks[index * 10: (index + 1) * 10]
            crawl_song_list = self.loop.run_until_complete(asyncio.wait(_tasks))
            for _ in crawl_song_list[0]:
                song_id, song_info = _.result()
                self.store_song_detail(song_id, song_info)
                print(f"crawled song_id: {song_id}, the song_info: {song_info}")
            sleep_time = random.uniform(1, 3)
            print(f"sleep {sleep_time} s")
            time.sleep(sleep_time)

    async def crawl_song_detail(self, song_id):
        """
        根据指定的歌曲id获取歌曲的详细信息
        :param song_id: 歌曲id
        """
        print(f"start crawl song: {song_id}")
        rand_key = self.rand_char()
        self.song_detail_data['rid'] = f'R_SO_4_{song_id}'
        post_data = {
            'params': self.gen_params(self.song_detail_data, rand_key),
            'encSecKey': self.gen_enc_sec_key(rand_key)
        }
        self.headers['Referer'] = f'https://music.163.com/song?id={song_id}'
        future = self.loop.run_in_executor(None, functools.partial(self.session.post, url=self.song_detail_url.format(song_id), data=post_data, headers=self.headers))
        resp = await future
        if resp.status_code == 200:
            return song_id, resp.json()
        else:
            return song_id, {}

    def store_song_detail(self, song_id, song_info):
        """
        入库歌曲详情
        :param song_id: 歌曲id
        :param song_info: 歌曲信息的数据
        """
        print(f"store song id {song_id} start   ------>>>>>>")
        insert_sql = '''insert into song_detail (song_id, comment_num, song_info) values (?, ?, ?)'''
        self.cursor.execute(
            insert_sql,
            (song_id, song_info.get('total', 0),
             json.dumps(song_info, ensure_ascii=False, separators=(',', ':'))))
        self.conn.commit()
        print(f"store song id {song_id} end   ------>>>>>>")


if __name__ == '__main__':
    n = NetEaseMusic()
    n.crawl_music_list_with_key_word('婚礼')
    n.crawl_song_list_from_sql()
    n.close_db_connection()
    n.loop.close()
    print('done')

#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2019/4/18 10:12
# @Author  : Xtmin
# @Email   : wangdaomin123@hotmail.com
# @File    : proxy_scan.py
# @Software: PyCharm
import sqlite3
import asyncio
import urllib3
import certifi
import requests
import functools
from scrapy.selector import Selector

session = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
tasks = []

loop = asyncio.get_event_loop()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


conn = sqlite3.connect('./ip_proxy.db')
conn.row_factory = dict_factory
cur = conn.cursor()

cur.execute('''create table if not exists proxy (id integer primary key not null,ip text not null, port text not null, type text not null, anonymous boolean, address text, speed text, is_alive boolean not null, last_check_time datetime, status text not null, source text not null, unique(ip, port))''')


async def proxies_checker(proxies, proxy_info):
    try:
        future = loop.run_in_executor(None, functools.partial(requests.get, 'http://pv.sohu.com/cityjson', proxies=proxies, timeout=10))
        resp = await future
        if proxy_info.get('ip') in resp.text:
            store_proxy(
                proxy_info.get('ip'),
                proxy_info.get('port'),
                proxy_info.get('type'),
                proxy_info.get('anonymous'),
                proxy_info.get('address'),
                proxy_info.get('speed'),
                True,
                'normal',
                proxy_info.get('source')
            )
    except Exception as e:
        print(f"proxy: {proxies.get('http')} can`t be uesd")
        store_proxy(
            proxy_info.get('ip'),
            proxy_info.get('port'),
            proxy_info.get('type'),
            proxy_info.get('anonymous'),
            proxy_info.get('address'),
            proxy_info.get('speed'),
            False,
            'destroy',
            proxy_info.get('source')
        )


# 西刺代理高匿
def xc_high_anonymous():
    resp = session.request('get', 'https://www.xicidaili.com/nn/')
    sel = Selector(text=resp.data.decode('utf8'))
    lst = sel.xpath('//table[@id="ip_list"]/tr[position()>1]')
    for _ in lst:
        ip = _.xpath('./td[2]/text()').extract_first()
        port = _.xpath('./td[3]/text()').extract_first()
        address = _.xpath('normalize-space(./td[4])').extract_first()
        type = _.xpath('./td[6]/text()').extract_first()
        speed = _.xpath('./td[7]/div/@title').extract_first()
        connect_time = _.xpath('./td[8]/div/@title').extract_first()
        living_time = _.xpath('./td[9]/text()').extract_first()
        last_check_time = _.xpath('./td[10]/text()').extract_first()

        print(f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}""")

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': True,
            'speed': speed,
            'connect_time': connect_time,
            'living_time': living_time,
            'last_check_time': last_check_time,
            'source': 'xici'
        }
        tasks.append(proxies_checker(proxies, proxy_info))

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


# 西刺代理普匿
def xc_normal_anonymous():
    resp = session.request('get', 'https://www.xicidaili.com/nt/')
    sel = Selector(text=resp.data.decode('utf8'))
    lst = sel.xpath('//table[@id="ip_list"]/tr[position()>1]')
    for _ in lst:
        ip = _.xpath('./td[2]/text()').extract_first()
        port = _.xpath('./td[3]/text()').extract_first()
        address = _.xpath('normalize-space(./td[4])').extract_first()
        type = _.xpath('./td[6]/text()').extract_first()
        speed = _.xpath('./td[7]/div/@title').extract_first()
        connect_time = _.xpath('./td[8]/div/@title').extract_first()
        living_time = _.xpath('./td[9]/text()').extract_first()
        last_check_time = _.xpath('./td[10]/text()').extract_first()

        print(f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}""")

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': False,
            'speed': speed,
            'connect_time': connect_time,
            'living_time': living_time,
            'last_check_time': last_check_time,
            'source': 'xici'
        }
        tasks.append(proxies_checker(proxies, proxy_info))

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


# 快代理高匿
def kuai_high_anonymous():
    resp = session.request('get', 'https://www.kuaidaili.com/free/inha/')
    sel = Selector(text=resp.data.decode('utf8'))
    lst = sel.xpath('//div[@id="list"]/table/tbody/tr')
    for _ in lst:
        ip = _.xpath('./td[1]/text()').extract_first()
        port = _.xpath('./td[2]/text()').extract_first()
        address = _.xpath('./td[5]/text()').extract_first()
        type = _.xpath('./td[4]/text()').extract_first()
        speed = _.xpath('./td[6]/text()').extract_first()
        connect_time = None
        living_time = None
        last_check_time = _.xpath('./td[7]/text()').extract_first()

        print(f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}""")

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': True,
            'speed': speed,
            'connect_time': connect_time,
            'living_time': living_time,
            'last_check_time': last_check_time,
            'source': 'kuai'
        }
        tasks.append(proxies_checker(proxies, proxy_info))

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


# 快代理普匿
def kuai_normal_anonymous():
    resp = session.request('get', 'https://www.kuaidaili.com/free/intr/')
    sel = Selector(text=resp.data.decode('utf8'))
    lst = sel.xpath('//div[@id="list"]/table/tbody/tr')
    for _ in lst:
        ip = _.xpath('./td[1]/text()').extract_first()
        port = _.xpath('./td[2]/text()').extract_first()
        address = _.xpath('./td[5]/text()').extract_first()
        type = _.xpath('./td[4]/text()').extract_first()
        speed = _.xpath('./td[6]/text()').extract_first()
        connect_time = None
        living_time = None
        last_check_time = _.xpath('./td[7]/text()').extract_first()

        print(f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}""")

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': False,
            'speed': speed,
            'connect_time': connect_time,
            'living_time': living_time,
            'last_check_time': last_check_time,
            'source': 'kuai'
        }
        tasks.append(proxies_checker(proxies, proxy_info))

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


def store_proxy(ip, port, type, anonymous=False, address=None, speed=None, is_alive=False, status='normal', source='local'):
    sql = '''replace into proxy (ip, port, type, anonymous, address, speed, is_alive, last_check_time, status, source) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), ?, ?)'''
    cur.execute(sql, (ip, port, type, anonymous, address, speed, is_alive, status, source))


def start_crawler():
    xc_high_anonymous()
    xc_normal_anonymous()
    kuai_high_anonymous()
    kuai_normal_anonymous()


if __name__ == '__main__':
    start_crawler()
    loop.close()
    cur.close()
    conn.close()
    print('Done')

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
from dateutil.utils import within_delta
from datetime import datetime, timedelta
from dateutil.parser import parse as du_parse

session = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                              ca_certs=certifi.where())
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

cur.execute('''
    create table if not exists proxy
        (
            id              integer primary key not null
        , ip              text not null
        , port            text not null
        , type            text not null
        , anonymous       text
        , address         text
        , speed           text
        , is_alive        boolean not null
        , last_check_time datetime
        , status          text not null
        , source          text not null
        , unique(ip, port)
        )
    ''')


async def proxies_checker(proxies, proxy_info):
    try:
        future = loop.run_in_executor(
            None,
            functools.partial(requests.get,
                              'http://pv.sohu.com/cityjson',
                              proxies=proxies,
                              timeout=10))
        resp = await future
        if proxy_info.get('ip') in resp.text:
            store_proxy(proxy_info.get('ip'), proxy_info.get('port'),
                        proxy_info.get('type'), proxy_info.get('anonymous'),
                        proxy_info.get('address'), proxy_info.get('speed'),
                        True, 'normal', proxy_info.get('source'))
    except Exception as e:
        print(f"proxy: {proxies.get('http')} can`t be uesd")
        store_proxy(proxy_info.get('ip'), proxy_info.get('port'),
                    proxy_info.get('type'), proxy_info.get('anonymous'),
                    proxy_info.get('address'), proxy_info.get('speed'), False,
                    'destroy', proxy_info.get('source'))


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

        print(
            f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}"""
        )

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': '高匿',
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

        print(
            f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}"""
        )

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': '透明',
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

        print(
            f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}"""
        )

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': '高匿',
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

        print(
            f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, connect_time: {connect_time}, living_time: {living_time}, last_check_time: {last_check_time}"""
        )

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': '透明',
            'speed': speed,
            'connect_time': connect_time,
            'living_time': living_time,
            'last_check_time': last_check_time,
            'source': 'kuai'
        }
        tasks.append(proxies_checker(proxies, proxy_info))

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


# 旗云代理
def qy():
    base_url = 'http://www.qydaili.com/free/?action=china&page=%s'
    index = 1
    time_now = datetime.now()
    continue_flag = True
    while continue_flag:
        url = base_url % index
        resp = session.request('get', url)
        sel = Selector(text=resp.data.decode('utf8'))
        lst = sel.xpath('//div[@class="container"]/table/tbody/tr')
        if not lst:
            continue_flag = False
        for item in lst:
            ip = item.xpath('./td[1]/text()').extract_first()
            port = item.xpath('./td[2]/text()').extract_first()
            _anonymous = item.xpath('./td[3]/text()').extract_first()
            anonymous = _anonymous if _anonymous != '匿名' else '普匿'
            type = item.xpath('./td[4]/text()').extract_first()
            address = item.xpath('./td[5]/text()').extract_first()
            speed = item.xpath('./td[6]/text()').extract_first()
            _last_check_time = item.xpath('./td[7]/text()').extract_first()
            last_check_time = du_parse(_last_check_time)
            if not within_delta(time_now, last_check_time, timedelta(days=1)):
                continue_flag = False
            print(
                f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, last_check_time: {last_check_time}"""
            )

            proxies = {
                'http': f'http://{ip}:{port}',
                'https': f'http://{ip}:{port}'
            }
            proxy_info = {
                'ip': ip,
                'port': port,
                'address': address,
                'type': type,
                'anonymous': anonymous,
                'speed': speed,
                'connect_time': None,
                'living_time': None,
                'last_check_time': last_check_time,
                'source': 'qiyun'
            }
            tasks.append(proxies_checker(proxies, proxy_info))
        index += 1

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


# 三一代理
def proxy_31():
    # http代理
    crawl_31('http://31f.cn/http-proxy/', 'HTTP')
    # https代理
    crawl_31('http://31f.cn/https-proxy/', 'HTTPS')

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


def crawl_31(url, type):
    resp = session.request('get', url)
    sel = Selector(text=resp.data.decode('utf8'))
    lst = sel.xpath('//table[@class="table table-striped"]/tr[position()>1]')
    for item in lst:
        ip = item.xpath('./td[2]/text()').extract_first()
        port = item.xpath('./td[3]/text()').extract_first()
        address = f"{item.xpath('normalize-space(./td[4])').extract_first()}-{item.xpath('normalize-space(./td[5])').extract_first()}"
        _anonymous = item.xpath('./td[7]/text()').extract_first()
        if _anonymous == 'transparent':
            anonymous = '透明'
        elif _anonymous == 'anonymous':
            anonymous = '普匿'
        else:
            anonymous = '高匿'
        speed = item.xpath('./td[8]/text()').extract_first()
        print(
            f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}"""
        )

        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}'
        }
        proxy_info = {
            'ip': ip,
            'port': port,
            'address': address,
            'type': type,
            'anonymous': anonymous,
            'speed': speed,
            'connect_time': None,
            'living_time': None,
            'last_check_time': None,
            'source': '31f'
        }
        tasks.append(proxies_checker(proxies, proxy_info))


# 89ip
def crawl_89ip():
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
    }
    base_url = 'http://www.89ip.cn/index_%s.html'
    index = 1
    time_now = datetime.now()
    continue_flag = True
    while continue_flag:
        url = base_url % index
        resp = session.request('get', url, headers=headers)
        sel = Selector(text=resp.data.decode('utf8'))
        lst = sel.xpath('//table[@class="layui-table"]/tbody/tr')
        if not lst:
            continue_flag = False
        for item in lst:
            ip = item.xpath('normalize-space(./td[1])').extract_first()
            port = item.xpath('normalize-space(./td[2])').extract_first()
            address = item.xpath('normalize-space(./td[3])').extract_first()
            _last_check_time = item.xpath(
                'normalize-space(./td[5])').extract_first()
            last_check_time = du_parse(_last_check_time)
            if not within_delta(time_now, last_check_time, timedelta(days=1)):
                continue_flag = False
            print(
                f"""ip: {ip}, port: {port}, address: {address}, last_check_time: {last_check_time}"""
            )

            proxies = {
                'http': f'http://{ip}:{port}',
                'https': f'http://{ip}:{port}'
            }
            proxy_info = {
                'ip': ip,
                'port': port,
                'address': address,
                'type': 'HTTP',
                'anonymous': '未知',
                'speed': None,
                'connect_time': None,
                'living_time': None,
                'last_check_time': last_check_time,
                'source': '89ip'
            }
            tasks.append(proxies_checker(proxies, proxy_info))

        index += 1

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


# 云代理
def proxy_ip3366():
    crawl_ip3366('http://www.ip3366.net/free/?stype=1')
    crawl_ip3366('http://www.ip3366.net/free/?stype=2')

    loop.run_until_complete(asyncio.wait(tasks))
    conn.commit()


def crawl_ip3366(url):
    base_url = f"{url}&page=%s"
    index = 1
    time_now = datetime.now()
    continue_flag = True
    while continue_flag:
        url = base_url % index
        resp = session.request('get', url)
        sel = Selector(text=resp.data.decode('gb2312'))
        lst = sel.xpath(
            '//table[@class="table table-bordered table-striped"]/tbody/tr')
        if not lst:
            continue_flag = False
        for item in lst:
            ip = item.xpath('./td[1]/text()').extract_first()
            port = item.xpath('./td[2]/text()').extract_first()
            _anonymous = item.xpath('./td[3]/text()').extract_first()
            if _anonymous == '高匿代理IP':
                anonymous = '高匿'
            elif _anonymous == '普通代理IP':
                anonymous = '普匿'
            else:
                anonymous = '透明'
            type = item.xpath('./td[4]/text()').extract_first()
            address = item.xpath('./td[5]/text()').extract_first()
            speed = item.xpath('./td[6]/text()').extract_first()
            _last_check_time = item.xpath('./td[7]/text()').extract_first()
            last_check_time = du_parse(_last_check_time)
            if not within_delta(time_now, last_check_time, timedelta(days=1)):
                continue_flag = False
            print(
                f"""ip: {ip}, port: {port}, address: {address}, type: {type}, speed: {speed}, last_check_time: {last_check_time}"""
            )

            proxies = {
                'http': f'http://{ip}:{port}',
                'https': f'http://{ip}:{port}'
            }
            proxy_info = {
                'ip': ip,
                'port': port,
                'address': address,
                'type': type,
                'anonymous': anonymous,
                'speed': speed,
                'connect_time': None,
                'living_time': None,
                'last_check_time': last_check_time,
                'source': 'ip3366'
            }
            tasks.append(proxies_checker(proxies, proxy_info))

        index += 1


def store_proxy(ip,
                port,
                type,
                anonymous=False,
                address=None,
                speed=None,
                is_alive=False,
                status='normal',
                source='local'):
    sql = '''replace into proxy (ip, port, type, anonymous, address, speed, is_alive, last_check_time, status, source) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), ?, ?)'''
    cur.execute(
        sql,
        (ip, port, type, anonymous, address, speed, is_alive, status, source))


def start_crawler():
    xc_high_anonymous()
    xc_normal_anonymous()
    kuai_high_anonymous()
    kuai_normal_anonymous()
    qy()
    proxy_31()
    crawl_89ip()
    proxy_ip3366()


if __name__ == '__main__':
    start_crawler()
    loop.close()
    cur.close()
    conn.close()
    print('Done')

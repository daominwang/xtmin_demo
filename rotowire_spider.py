#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/10 13:32
# @Author  : xtmin
# @Email   : wangdaomin123@hotmail.com
# @File    : rotowire_spider.py
# @Software: PyCharm
import re
import sys
import random
import logging
import pymongo  # 采用mongodb 3.6.3
import requests
from lxml import etree
from datetime import timedelta
from dateutil.parser import parse as d_parse
from apscheduler.schedulers.blocking import BlockingScheduler

re_patten = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}')
mongodb_host = '127.0.0.1'
mongodb_port = 27017

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

headers = {
    'Host': 'www.rotowire.com',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-User': '?1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,und;q=0.7',
}


def init_logger():
    """
    初始化日志文件对象，及相关配置
    :return:
    """
    logger = logging.getLogger(__name__)
    format_pattern = '[%(asctime)s %(levelname)s] %(filename)s:%(lineno)s %(message)s'
    log_formatter = logging.Formatter(format_pattern)
    logging.basicConfig(level=logging.INFO, format=format_pattern)
    file_handler = logging.FileHandler('rotowire_spider.log', encoding='utf8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
    return logger


logger = init_logger()


def init_mongodb():
    """
    初始化mongodb连接
    :return:
    """
    try:
        mongo_client = pymongo.MongoClient(mongodb_host, mongodb_port)
        mongo_db = mongo_client['rotowire']
        team_info_col = mongo_db['team_info']
        logger.debug(f'init mongodb {mongodb_host}:{mongodb_port} finish')
        return mongo_client, team_info_col
    except Exception as e:
        logger.error(f'init mongodb error   {e.args}')
        sys.exit(0)


def close_mongodb(mongo_client):
    """
    关闭mongodb连接
    :param mongo_client:
    :return:
    """
    mongo_client.close()
    logger.debug('close mongodb')


def get_player_list(player_etree):
    """
    从球员的ElementTree对象中提取球员信息
    :param player_etree:
    :return:
    """
    player_list = []
    for player in player_etree:
        player_name = player.xpath('./a/text()')[0]
        player_position = player.xpath('./div[@class="lineup__pos"]/text()')[0]
        reason = player.xpath('./span[@class="lineup__inj"]/text()')
        player_info = {
            'name': player_name,
            'position': player_position
        }
        if reason:
            player_info['reason'] = reason[0]
        player_list.append(player_info)
        logger.debug(f'extract player {player_info}')
    return player_list


def get_utc_time(match_date, match_time):
    """
    将提供的采集到的日期和时间处理为UTC格式时间字符串
    :param match_date:
    :param match_time:
    :return:
    """
    utc_match_time = d_parse(f'{match_date} {match_time}') + timedelta(hours=5)
    return utc_match_time.strftime('%Y-%m-%d %H:%M:%S')


def push_data_list_to_mongo(team_info_list):
    """
    将采集到的比赛球队信息列表入库mongodb
    :param team_info_list:
    :return:
    """
    mongo_client, team_info_col = init_mongodb()
    for team_info in team_info_list:
        # 查询入库数据是否重复，依据比赛时间和球队名称，如果比赛时间和球队名称重复，则表示该条数据已经入库，不再重复入库
        if not team_info_col.find_one({'match_time': team_info.get('match_time'), 'team_name': team_info.get('team_name')}):
            logger.info(f'insert team: {team_info}')
            team_info_col.insert_one(team_info)
        else:
            logger.debug('duplicate data with %s' % {'match_time': team_info.get('match_time'), 'team_name': team_info.get('team_name')})
    mongo_client.close()


def crawl_url(url):
    """
    对指定url发送请求，并返回response对象
    :param url:
    :return:
    """
    headers['User-Agent'] = random.choice(ua_list)
    resp = requests.get(url, headers=headers)
    logger.info(f'crawl {url} status_code {resp.status_code}')
    return resp


def extract_player_info(match_meta, utc_match_time, team_type):
    """
    提取球队信息及球员信息
    :param match_meta: 比赛元数据，ElementTree类型
    :param utc_match_time: 格式化处理后的UTC时间字符串
    :param team_type: 球队类型
                      visit --- 客队
                      home  --- 主队
    :return: 球队信息 dict类型
    """
    team_info = {
        'team_type': team_type,
        'match_time': utc_match_time,
        'team_name': match_meta.xpath(f'.//a[@class="lineup__team is-{team_type}"]/div/text()')[0],
        'status': match_meta.xpath(f'normalize-space(string(.//ul[@class="lineup__list is-{team_type}"]/li[contains(@class, "lineup__status")]))')[0]
    }

    visit_player_list = match_meta.xpath(f'.//ul[@class="lineup__list is-{team_type}"]/li[text()="INJURIES"]/preceding-sibling::li[@class="lineup__player"]')
    team_info['lineup_player'] = get_player_list(visit_player_list)

    visit_injure_player_list = match_meta.xpath(f'.//ul[@class="lineup__list is-{team_type}"]/li[text()="INJURIES"]/following-sibling::li[@class="lineup__player"]')
    team_info['injure_players'] = get_player_list(visit_injure_player_list)
    return team_info


def crawler():
    """
    爬虫主体
    :return:
    """
    logger.debug('start crawler')
    resp = crawl_url('https://www.rotowire.com/basketball/nba-lineups.php')

    elem_tree = etree.HTML(resp.text)
    match_meta_list = elem_tree.xpath('//main[@class="page"]/div[contains(@class, "lineups")]/div[contains(@class, "lineup is-nba") and not(contains(@class, "is-tools"))]')

    logger.info(f'get {len(match_meta_list)} matches')

    team_info_list = []

    for match_meta in match_meta_list:
        ticket_url = match_meta.xpath('./div[@class="lineup__meta flex-row"]/a[1]/@href')[0]
        match_date = re_patten.findall(ticket_url)[0]
        match_time = match_meta.xpath('./div[@class="lineup__meta flex-row"]/div[@class="lineup__time"]/text()')[0]
        match_time = match_time.replace('ET', '')
        utc_match_time = get_utc_time(match_date, match_time)

        # visit team info
        visit_team_info = extract_player_info(match_meta, utc_match_time, 'visit')
        logger.debug(f'extract team info: {visit_team_info}')
        # home team info
        home_team_info = extract_player_info(match_meta, utc_match_time, 'home')
        logger.debug(f'extract team info: {home_team_info}')

        team_info_list.append(visit_team_info)
        team_info_list.append(home_team_info)

    logger.debug('end crawler')

    logger.debug('start push to database')
    push_data_list_to_mongo(team_info_list)
    logger.debug('end push to database')


if __name__ == '__main__':
    # 创建任务调度器实例
    scheduler = BlockingScheduler()
    # 启动脚本的时候先执行一次爬取任务
    scheduler.add_job(crawler)
    # 间隔调度，每20分钟执行一次
    scheduler.add_job(crawler, 'interval', minutes=20)
    # 启动调度器
    scheduler.start()

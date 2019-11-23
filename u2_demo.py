#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 10:34
# @Author  : Xtmin
# @Email   : wangdaomin123@hotmail.com
# @File    : u2_demo.py
# @Software: PyCharm
import time
import uiautomator2 as u2
from adbutils import AdbClient

devices = AdbClient().device_list()
d = u2.connect(devices[0].serial)

d.watcher("ALERT").when(text="Warning: Invalid CERT Authority").click(text='YES')

d_info = d.info
width, height = d_info.get('displayWidth'), d_info.get('displayHeight')

# 阅读10篇文章
d(resourceId="cn.xuexi.android:id/home_bottom_tab_button_work").wait(timeout=5)
d(resourceId="cn.xuexi.android:id/home_bottom_tab_button_work").click()
lst = d(resourceId="cn.xuexi.android:id/general_card_title_id")
print(len(lst))
hits = 1
while hits != 10:
    for index in range(len(lst)):
        if not lst[index].sibling(resourceId="cn.xuexi.android:id/st_feeds_card_play").exists:
            lst[index].click()
            d(resourceId="cn.xuexi.android:id/common_webview").wait(timeout=5)
            time.sleep(1)
            # print(d.xpath('//android.widget.FrameLayout[@resource-id="cn.xuexi.android:id/common_webview"]/android.view.View/android.view.View[2]').get().info.get('contentDescription'))
            # 翻页阅读
            print('翻页开始')
            i = 0
            while not d(text='观点').exists:
                if i != 0:
                    print(f"第{i}次翻页")
                d.drag(width/2, height-150, width/2, height/2)
                i += 1
            # d(resourceId="cn.xuexi.android:id/common_webview").scroll.to(text="观点")
            print('翻页结束')
            if hits in (1, 2):
                # 收藏
                d.wait_timeout = 5
                d.xpath('//*[@resource-id="android:id/content"]/android.widget.FrameLayout[1]/android.widget.FrameLayout[1]/android.widget.FrameLayout[1]/android.widget.LinearLayout[1]/android.widget.LinearLayout[1]/android.widget.ImageView[1]').click()
                # 分享
                d.xpath('//*[@resource-id="android:id/content"]/android.widget.FrameLayout[1]/android.widget.FrameLayout[1]/android.widget.FrameLayout[1]/android.widget.LinearLayout[1]/android.widget.LinearLayout[1]/android.widget.ImageView[2]').click()
                d(resourceId="cn.xuexi.android:id/txt_gv_item", text="分享到学习强国").click()
                d(text="创建新的聊天").wait(20)
                d(resourceId="cn.xuexi.android:id/session_item").click()
                d(resourceId="cn.xuexi.android:id/parentPanel").wait(20)
                d(text="发送").click()
                d(text="欢迎发表你的观点").click()
                d(text="好观点将会被优先展示").click()
                d.set_fastinput_ime(True)
                d.send_keys("愿祖国繁荣昌盛")
                d.set_fastinput_ime(False)
                d(text="发布").click()
            # 阅读完成，计数并返回上一页
            hits += 1
            d.press('back')
            if hits == 10:
                break
            d(resourceId="cn.xuexi.android:id/general_card_title_id").wait(timeout=5)

    # 全部阅读完成，翻页
    d(scrollable=True).scroll()


d.close()
print('done')

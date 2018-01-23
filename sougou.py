# -*- coding: UTF-8 -*-
"""
搜狗微信爬虫，自动创建并存入mysql数据库
"""
import re
import time
import pymysql
import requests
from lxml import etree

#IP被封后，通过输入验证码获取新的SNUID
def captcha():
    #url为任意一个网址（被封后任意同一host网址均跳转到验证码页面）
    url = 'http://weixin.sogou.com/weixin?query=%E6%92%92%E6%97%A6&type=2&page=2'
    #错误重试
    while True:
        try:
            res = s.get(url)
            selector = etree.HTML(res.text)
            captcha = selector.xpath('//img[@id="seccodeImage"]/@src')[0]
            captchaurl = 'http://weixin.sogou.com/antispider/' + captcha
            #获取验证码图片
            captchares = s.get(captchaurl)
            fp = open('C:\Users\Administrator\Desktop/1/12.jpg', 'wb')
            fp.write(captchares.content)
            fp.close()
            #输入验证码
            captchainput = raw_input("请输入验证码")
            data = {'c': captchainput,
                    'r': '%2Fweixin%3Fquery%3D%E6%92%92%E6%97%A6%26type%3D2%26page%3D2', 'v': '5'}
            posturl = 'http://weixin.sogou.com/antispider/thank.php'
            #提交验证码
            lastres = s.post(posturl, data=data)
            #正则提取提交验证码后的网页SNUID值
            SNUID = re.findall('"id":\s"(.*?)"', lastres.content)[0]
            break
        except IndexError:
            print "验证码错误重新输入验证码"
    #将解封后的cookie存入headers
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
        'cookie': '%s;SNUID=%s' % (NO_SNUID_COOKIE,SNUID)}
    #返回headers
    return headers

#获取响应且禁止重定向
def get_response(url,headers):
    res = s.get(url,headers=headers,allow_redirects=False)
    return res

#主程序运行
if __name__ == '__main__':
    COOKIE = raw_input("请输入登陆后的COOKIE--->")
    time.sleep(0.2)
    #剔除cookie中的SNUID
    NO_SNUID_COOKIE = re.sub('SNUID=.*?;','',COOKIE)
    #输入获取信息
    while True:
        key_word = raw_input("请输入要查找的关键词--->")
        time.sleep(0.2)
        start_page = raw_input("请输入1-100之间的起始页码--->")
        time.sleep(0.2)
        end_page = raw_input("请输入1-100之间终止页码--->")
        time.sleep(0.2)
        if end_page>start_page:
            break
        else:
            print "输入错误，请重新输入"
    start_time = time.time()
    i = 1
    #创建会话
    s = requests.session()
    #替换新头部
    headers = captcha()
    #连接数据库
    conn = pymysql.connect(host='localhost', port=3306, db='db', user='root', passwd='passwd', charset='utf8')
    cursor = conn.cursor()
    #创建数据表
    sql_create_table = 'create table sougou_{}(id int key auto_increment,title text,introduction text,author text,article_url text,publish_time text)'.format(key_word)
    cursor.execute(sql_create_table)
    urls = ['http://weixin.sogou.com/weixin?query={}&type=2&page={}'.format(key_word,num) for num in range(int(start_page),int(end_page)+1)]
    for url in urls:
        print '正在爬取第%s页'%(i)
        res = get_response(url,headers=headers)
        #判断IP是否被封
        if res.status_code==302:
            headers = captcha()
            res = get_response(url,headers)
        selector = etree.HTML(res.text)
        infos = selector.xpath('//ul[@class="news-list"]/li')
        print infos
        i+=1
        for info in infos:
            # 获取文章标题
            titles = info.xpath('div[@class="txt-box"]/h3/a')[0]
            title = titles.xpath('string(.)')
            # 获取文章简介
            introductions = info.xpath('div[@class="txt-box"]/p')
            if not introductions:
                print '暂无简介'
            else:
                introduction = introductions[0].xpath('string(.)')
            # 获取文章作者
            author = info.xpath('div[@class="txt-box"]/div/a/text()')[0]
            # 获取文章链接
            article_url = info.xpath('div[@class="txt-box"]/h3/a/@href')[0]
            title_res = get_response(article_url,headers=headers)
            selector_res = etree.HTML(title_res.text)
            # 获取文章发布时间
            publish_time = selector_res.xpath('//*[@id="post-date"]/text()')
            if not publish_time:
                publish_time="暂无发表日期信息"
            else:
                publish_time=selector_res.xpath('//*[@id="post-date"]/text()')[0]
            #写入数据
            sql = 'insert into sougou_{}(title,introduction,author,article_url,publish_time) values(%s,%s,%s,%s,%s)'.format(key_word)
            cursor.execute(sql, (title, introduction, author, article_url, publish_time))
            conn.commit()
    # 断开数据库连接
    cursor.close()
    conn.close()
    end_time = time.time()
    time = end_time-start_time
    print "%s关键词%s页爬取完成，总共用时%s"%(key_word,i-1,time)
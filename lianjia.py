# -*- coding: UTF-8 -*-
"""
链家北京二手房信息,存入自动创建mysql数据表并存入
"""

import Queue
import time
from lxml import etree
import pymysql
import requests

dict_key_word = {
                 '东城':'dongcheng','西城':'xicheng','朝阳':'chaoyang','海淀':'haidian',
                 '丰台':'fengtai','石景山':'shijingshan','通州':'tongzhou','昌平':'changping',
                 '大兴':'daxing','亦庄开发区':'yizhuangkaifaqu','顺义':'shunyi','房山':'fangshan',
                 '门头沟':'mentougou','平谷':'pinggu','怀柔':'huairou','密云':'miyun','延庆':'yanqing',
                 }

def create_queue():
    # for i in range(1,9):
    #   exec'q%d=Queue.Queue()'%i
    for i in range(1, 9):
        globals()['q'+str(i)] = Queue.Queue()

def get_response(url):
    headers = {'user-agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
    res = requests.get(url,headers=headers)
    return res

def get_infos(res):
    selector = etree.HTML(res.text)
    infos = selector.xpath('//li[@class="clear"]')
    return infos

def get_title(infos):
    for info in infos:
        title = info.xpath('div[1]/div[1]/a/text()')[0]
        q1.put(title)
    return q1

def get_community(infos):
    for info in infos:
        community = info.xpath('div[1]/div[2]/div[1]/a/text()')[0]
        q2.put(community)
    return q2

def get_pattern(infos):
    for info in infos:
        pattern = info.xpath('div[1]/div[2]/div[1]/text()')[0]
        q3.put(pattern)
    return q3

def get_square(infos):
    for info in infos:
        square = info.xpath('div[1]/div[2]/div[1]/text()')[1]
        q4.put(square)
    return q4

def get_floor(infos):
    for info in infos:
        floor = info.xpath('div[1]/div[3]/div[1]/text()')[0]
        q5.put(floor)
    return q5

def get_position(infos):
    for info in infos:
        position = info.xpath('div[1]/div[3]/div[1]/a/text()')[0]
        q6.put(position)
    return q6

def get_price(infos):
    for info in infos:
        price = info.xpath('div[1]/div[4]/div[3]/div[1]/span[1]/text()')[0] + '万元'
        q7.put(price)
    return q7

def get_unit_price(infos):
    for info in infos:
        unit_price = info.xpath('div[1]/div[4]/div[3]/div[2]/span[1]/text()')[0]
        q8.put(unit_price)
    return q8

def get_info_number(start_url):
    res = get_response(start_url)
    selector = etree.HTML(res.text)
    numbers = selector.xpath('//h2[@class="total fl"]/span/text()')[0]
    return numbers

def connet_mysql():
    conn = pymysql.connect(host='localhost', user='root', passwd='passwd', port=3306, db='db', charset='utf8')
    return conn

def create_mysql(cursor):
    cursor.execute('drop table if EXISTS 链家_{}'.format(key_word))
    cursor.execute('create table 链家_{}(id int key auto_increment,title text,community text,pattern text,square text,floor text,position text,price text,unit_price text)'.format(key_word))

def main(url):
    res = get_response(url)
    infos = get_infos(res)
    title = get_title(infos)
    community = get_community(infos)
    pattern = get_pattern(infos)
    square = get_square(infos)
    floor = get_floor(infos)
    position = get_position(infos)
    price = get_price(infos)
    unit_price = get_unit_price(infos)
    while not q1.empty():
        title = q1.get()
        community = q2.get()
        pattern = q3.get()
        square = q4.get()
        floor = q5.get()
        position = q6.get()
        price = q7.get()
        unit_price = q8.get()
        sql = 'insert into 链家_{}(title,community,pattern,square,floor,position,price,unit_price) values(%s,%s,%s,%s,%s,%s,%s,%s)'.format(key_word)
        cursor.execute(sql, (title, community, pattern, square, floor, position, price, unit_price))
        conn.commit()

if __name__ == '__main__':
    while True:
        try:
            key_word = raw_input('请输入查询的区县--->')
            input = dict_key_word[key_word]
            break
        except KeyError:
            print "输入关键词错误，请重新输入"
    a = time.time()
    j=1
    create_queue()
    conn = connet_mysql()
    cursor = conn.cursor()
    create_mysql(cursor)
    start_url = 'https://bj.lianjia.com/ershoufang/{}/'.format(input)
    numbers = get_info_number(start_url)
    number = int(numbers)/30
    urls = ['https://bj.lianjia.com/ershoufang/{}/pg{}/'.format(input,num) for num in range(1,int(number)+2)]
    for url in urls:
        print '正在爬取第%d页'%j
        main(url)
        j+=1
    cursor.close()
    conn.close()
    b = time.time()
    time = b-a
    print "爬取完成，共爬取%d页，用时%s秒,爬取信息%s条"%(int(number)+1,time,int(numbers))
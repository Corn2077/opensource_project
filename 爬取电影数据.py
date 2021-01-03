#爬取电影数据

import requests
from lxml import etree
import pymysql
import traceback


def connectMysql():
    return pymysql.connect(host='localhost', user='root', password='tiger', port=3306, db='pluto')


def createMysqlTable(name):
    db = connectMysql()
    cursor = db.cursor()

    # fieldnames = ['年度排名','历史排名','电影名称','总导演','类型','票房','上映时间','主演']
    sql = 'create table if not exists ' + name + ' (    年度排名 int not null ,    历史排名 int not null ,    电影名称 varchar(255) not null ,    导演 varchar(1023) ,    类型 varchar(1023) ,    票房 varchar(255) not null,    上映时间 varchar(255) ,    主演 varchar(1023) ,    primary key(年度排名))'
    cursor.execute(sql)

    print('数据库创建成功')
    db.close()


def getHtmlText(url):
    headers = {
        'User-Agent': 'Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 84.0.4147.89 Safari / 537.36 Edg / 84.0.522.44 ',
        'Cookie':'DIDA642a4585eb3d6e32fdaa37b44468fb6c=stjkt9r49ivi1n263cuk50p922;Hm_lvt_e71d0b417f75981e161a94970becbb1b=1608471503;Hm_lpvt_e71d0b417f75981e161a94970becbb1b=1608473631;time=MTEzNTI2LjIxNjM0Mi4xMDI4MTYuMTA3MTAwLjExMTM4NC4yMDc3NzQuMTE5OTUyLjExMTM4NC4xMDQ5NTguMTE1NjY4LjEwMjgxNi4xMTk5NTIuMTExMzg0LjExNzgxMC4xMDkyNDIuMTE1NjY4LjEwNzEwMC4xMDkyNDIuMA'
    }
    try:
        result = requests.get(url, headers=headers, timeout=30)
        result.raise_for_status()  # 用来在发生异常的时候抛出异常
        result.encoding = result.apparent_encoding
        return result.text
    except requests.HTTPError:  # 接受异常
        return ""


def parsePage(html):
    tmp = []
    tmp2 = []  # 票房数据
    tmp3 = []  # 导演
    tmp4 = []  # 类型
    tmp5 = []  # 上映时间
    tmp6 = []  # 主演
    ttmp = ""
    clist = []  # 数据汇总临时变量
    rlist = []  # 数据汇总结果
    newhtml = etree.HTML(html, etree.HTMLParser())
    st = "最新票房 "
    headers = {
        'User-Agent': 'Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 84.0.4147.89 Safari / 537.36 Edg / 84.0.522.44 ',
        'Cookie':'DIDA642a4585eb3d6e32fdaa37b44468fb6c=stjkt9r49ivi1n263cuk50p922;Hm_lvt_e71d0b417f75981e161a94970becbb1b=1608471503;Hm_lpvt_e71d0b417f75981e161a94970becbb1b=1608473631;time=MTEzNTI2LjIxNjM0Mi4xMDI4MTYuMTA3MTAwLjExMTM4NC4yMDc3NzQuMTE5OTUyLjExMTM4NC4xMDQ5NTguMTE1NjY4LjEwMjgxNi4xMTk5NTIuMTExMzg0LjExNzgxMC4xMDkyNDIuMTE1NjY4LjEwNzEwMC4xMDkyNDIuMA'
    }
    content = newhtml.xpath('//*[@id="content"]/div[3]/table/tbody/tr/td[position()<=3]//text()')  # 爬取文本的前三条结果
    path = newhtml.xpath('//*[@id="content"]/div[3]/table/tbody/tr/td[position()=3]/a/@href')  # 爬取包含电影代码的字符串属性
    for i in range(len(path)):
        tmp.append(path[i].replace("/film/", ""))

    for t in tmp:
        url_tmp = 'http://58921.com/film/{num}/boxoffice'.format(num=t)
        response = requests.get(url_tmp, headers=headers, timeout=30)
        response.encoding = response.apparent_encoding
        html_temp = response.text
        newhtml_temp = etree.HTML(html_temp, etree.HTMLParser())
        piaofang = newhtml_temp.xpath('//*[@id="content"]/div[2]/div[2]/div/div/h3//text()')  # 爬取包含电影票房数据的字符串属性
        for i in range(len(piaofang)):
            tmp2.append(piaofang[i][piaofang[i].index(st):-1].replace(st, ""))

    for tt in tmp:
        url_tmp2 = 'http://58921.com/film/{num}'.format(num=tt)
        response2 = requests.get(url_tmp2, headers=headers, timeout=30)
        response2.encoding = response2.apparent_encoding
        html_temp2 = response2.text
        newhtml_temp2 = etree.HTML(html_temp2, etree.HTMLParser())
        daoyan = newhtml_temp2.xpath('//*[@class="media-body"]//li[contains(string(),"导演：")]/a//text()')  # 爬取电影导演的字符串属性
        leixing = newhtml_temp2.xpath(
            '//*[@class="media-body"]//li[contains(string(),"类型：")]/a//text()')  # 爬取电影类型的字符串属性
        shijian = newhtml_temp2.xpath(
            '//*[@class="media-body"]//li[contains(string(),"上映时间：")]//text()')  # 爬取电影上映时间的字符串属性
        zhuyan = newhtml_temp2.xpath('//*[@class="media-body"]//li[contains(string(),"主演：")]/a//text()')  # 爬取电影主演的字符串属性
        for i in range(len(daoyan)):
            ttmp = ttmp + daoyan[i] + ' '
        tmp3.append(ttmp.strip())
        ttmp = ''
        for i in range(len(leixing)):
            ttmp = ttmp + leixing[i] + ' '
        tmp4.append(ttmp.strip())
        ttmp = ''
        for i in range(len(shijian)):
            ttmp = ttmp + shijian[i] + ' '
        tmp5.append(ttmp.replace('上映时间： ', "").strip())
        ttmp = ''
        for i in range(len(zhuyan)):
            ttmp = ttmp + zhuyan[i] + ' '
        tmp6.append(ttmp.strip())
        ttmp = ''

    length = len(content)
    weight = int(length / 3)  # 文字信息共有3条（年度排名，历史排名，电影名称）
    for i in range(weight):
        for j in range(3):
            clist.append(content[i * 3 + j])
        clist.append(tmp3[i])  # 总导演
        clist.append(tmp4[i])  # 电影类型
        clist.append(tmp2[i])  # 票房
        clist.append(tmp5[i])  # 时间
        clist.append(tmp6[i])  # 主演
        rlist.append(clist)
        clist = []
    return rlist


def mysqlData(name, datas):
    table = name
    keys = '年度排名,历史排名,电影名称,导演,类型,票房,上映时间,主演'

    db = connectMysql()
    cursor = db.cursor()

    for data in datas:
        values = ','.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES({values})'.format(table=table, keys=keys, values=values)
        try:
            if cursor.execute(sql, tuple(data)):
                db.commit()
        except pymysql.Error:
            traceback.print_exc()
            print("Failed")
            db.rollback()

    db.close()


def main():
    name = "2018票房数据"
    createMysqlTable(name)
    i = 0
    while i <= 6:
        url = "http://58921.com/alltime/2018?page=" + str(i)
        print(url)
        html = getHtmlText(url)
        rlist = parsePage(html)
        mysqlData(name, rlist)
        i = i + 1

main()

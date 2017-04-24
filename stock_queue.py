'''爬虫要先执行该段代码，获取所有股票代码，再根据代码获取所有股票评论'''

import requests
from spider.UA import agents
import time
import random
from multiprocessing import Pool
import json
from spider.db import StockMongo


'''爬虫的第一步，要先爬取的股票代码'''

headers={
    'User-Agent':random.choice(agents),
}

xueqiu_url='https://xueqiu.com/'#雪球官网
stocks_url='https://xueqiu.com/stock/cata/stocklist.json?page={page}&size=90&order=desc&orderby=percent&type=11%2C12&_={real_time}'#股票列表网

a=time.time()
real_time=str(a).replace('.','')[0:-1]
'''这个是访问雪球股票网页或者评论时后面的参数，测试了几次不加也是可以正常返回，但是为了保险还是参数原封不动加上,毕竟要大规模爬取'''


def get_data(num):#默认是不使用dialing
    Stock_database=StockMongo('xueqiu','stocks_list')
    url=stocks_url.format(page=str(num),real_time=real_time)#股票列表URL
    while True:
        session = requests.session()
        proxy = requests.get('http://localhost:5000/get').text  # 获取本地代理池代理
        proxies = {'http': 'http://{}'.format(proxy),
                   'https': 'http://{}'.format(proxy), }
        session.proxies = proxies  # 携带代理
        try:
            html = session.get(url='https://xueqiu.com/', headers=headers)
            stocks_list = session.get(url, headers=headers)
            if stocks_list.status_code == 200:
                stocks=json.loads(stocks_list.text)['stocks']
                for stork in stocks:
                    current=stork.get('current'),#获取价格
                    name=stork.get('name')
                    symbol=stork.get('symbol')
                    Stock_database.push_stocks(symbol=symbol,name=name,current_price=current)
                    print(current,name,symbol)
                break
        except:
            print('获取失败，准备重新获取')#失败后要
            time.sleep(2)
            continue

if __name__ =='__main__':
    pool =Pool(6)
    for i in range(1,59):
        pool.apply_async(func=get_data,args=(i,))

    pool.close()
    pool.join()#必须等待所有子进程结束




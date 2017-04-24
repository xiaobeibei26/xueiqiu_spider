import requests
from spider.UA import agents
import random
import time
import json
from spider.stock_queue import StockMongo
from lxml import html
import multiprocessing
from spider.thread_pool import ThreadPool#自己写的线程池


headers={
'User-Agent':random.choice(agents)
}



xueqiu_url='https://xueqiu.com/'#雪球官网
comment_url= 'https://xueqiu.com/statuses/search.json?count=10&comment=0&symbol={symbol}&hl=0&source=user&sort=time&page={page}&_={real_time}'









def get_comment():#默认是不使用dialing
    Stock_database = StockMongo('xueqiu', 'stocks_list')#链接第一步抓取的股票代码数据表，根据股票代码抓取评论
    # symbol = Stock_database.pop()  # 获取股票代码
    comment_database=StockMongo('xueqiu','comment_list')#评论要放进的数据库
    a = time.time()
    real_time = str(a).replace('.', '')[0:-1]#获取当前时间

    def thread_get_comment(num):
        while True:
            session = requests.session()
            proxy = requests.get('http://localhost:5000/get').text  # 获取本地代理池代理
            if proxy:
                proxies = {'http': 'http://{}'.format(proxy),
                           'https': 'http://{}'.format(proxy), }
                session.proxies = proxies  # 携带代理
                try:
                    url = comment_url.format(symbol=symbol, page=str(num[0]), real_time=real_time)  # 股票列表URL
                    # print(url)
                    '''利用线程池传进去的num是个元组，必须提取出来'''
                    First_request = session.get(url='https://xueqiu.com/', headers=headers, timeout=10)
                    comments_list = session.get(url, headers=headers, timeout=10)
                    if Stock_database.check_status(symbol):#如果已经爬取完成，直接结束循环
                        break

                    if str(comments_list.status_code) == str(200):#是否正常返回数据
                        stocks_comment = json.loads(comments_list.text)['list']
                        page = json.loads(comments_list.text)['maxPage']#获取最大页数
                        # print(page,num[0])
                        for stork in stocks_comment:
                            try:
                                text = stork.get('text').strip()
                                selector = html.fromstring(text)  # 里面的标签各种各样，各种嵌套，用正则调了很久，投降了，改用xpath
                                comment = selector.xpath('string(.)')
                                user_id = stork.get('user_id')  # 评论者ID
                                user = stork.get('user')  # 评论者信息
                                title = stork.get('title')  # 标题
                                stock_code = symbol  # 股票代码
                                comment_id = stork.get('id')  # 每条评论都要唯一的ID
                                comment_database.push_stock_comment(comment_id=comment_id,symbol=stock_code,comment=comment, user_id=user_id,
                                                                    user=user,title=title)
                                print('正在爬取第',num[0],'该股票一共',page)
                                if str(page) == str(num[0]):#抓取到了最后一页
                                    print(symbol,'该股票抓取成功')
                                    Stock_database.complete(symbol=symbol)
                                    break
                                break
                            except:
                                pass
                        break
                except  Exception as e:
                    # print('获取失败，准备重新获取')  # 失败后再来
                    time.sleep(10)
                    continue
            else:

                time.sleep(15)  # 等待重新获取代理
                continue


    def comment_crawler():
        pool=ThreadPool(6)
        for num  in range(1,101):#遍历一到100页
            pool.run(func=thread_get_comment,args=(num,))

    while Stock_database:
        try:
            symbol = Stock_database.pop()
            comment_crawler()
            time.sleep(2)
        except KeyError:#队列没有数据了
            print('队列没有数据')
            break




def process_crawler():
    process=[]
    num_cups=multiprocessing.cpu_count()
    print('将会启动的进程数为',num_cups)
    for i in range(int(num_cups)-2):
        p=multiprocessing.Process(target=get_comment)#创建进程
        p.start()
        process.append(p)
        for p in process:
            p.join()
if __name__ == '__main__':
    process_crawler()

















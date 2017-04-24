# from spider.stock_queue import StockMongo
# Stock_database=StockMongo('xueqiu','stocks_list')
# Stock_database.status_setting()
# import requests
# proxy = requests.get('http://localhost:5000/get').text  # 获取本地代理池代理
# print(proxy)
# real_proxy = {'http': 'http://{}'.format(proxy),
#               'https': 'http://{}'.format(proxy), }
# print(real_proxy)
#


'''这是线程池'''
import queue
import time
import threading
StopEvent = []

class ThreadPool():
    def __init__(self, max_num):
        self.max_num = max_num  # 最多创建的线程数
        self.generate_list = []  # 真实创建的线程数
        self.free_list = []
        self.q = queue.Queue()

    def run(self, func, args, callback=None):

        data = (func, args, callback)  # 将任务封装进一个元组
        self.q.put(data)  # 将任务放进队列
        if len(self.free_list) == 0 and len(self.generate_list) < self.max_num:
            self.work(data)

    def work(self, data):

        thread = threading.Thread(target=self.call)  # 创建线程
        thread.start()

    def call(self):
        current_thread = threading.currentThread
        self.generate_list.append(current_thread)  # 把现有的线程添加进去

        job = self.q.get()  # 去队列里面领取任务
        while job != StopEvent:
            func, args, callback = job
            try:  # 执行任务
                ret = func(args)
            except Exception as e:
                print(e)
            else:  # 如果有回调函数则执行
                if callback:
                    callback()
                else:
                    pass

            self.free_list.append(current_thread)  # 任务执行完成则添加进空闲线程
            job = self.q.get()
            self.free_list.remove(current_thread)  # 获得了任务则从空闲列表中去除

        else:
            self.generate_list.remove(current_thread)  # 清除该线程，让Python垃圾回收机制处理

    def close(self):
        for itme in range(len(self.generate_list)):
            self.q.put(StopEvent)



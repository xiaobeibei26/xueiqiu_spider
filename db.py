from pymongo import MongoClient,errors
from _datetime import datetime,timedelta



class StockMongo(object):
    OUTSIANDING = 1
    PROCESSING = 2
    COMPLETE = 3
    def __init__(self,db,collection,timeout=300):
        self.client = MongoClient()
        self.databese=self.client[db]
        self.db=self.databese[collection]
        self.timeout = timeout

    def __bool__(self):
        record = self.db.find_one(
            {'status': {'$ne': self.COMPLETE}}
        )
        return True if record else False


    def push_stocks(self,symbol,name,current_price):#将股票代码插进数据库，以备爬取评论的时候提取
        try:
            self.db.insert({'_id':symbol,'status':self.OUTSIANDING,'股票名字':name,'股票现价':current_price})#股票代码作为表的ID
            print(symbol,name,'股票插入成功')
        except errors.DuplicateKeyError as  e:#这里预防万一，怕股票有重复
            print(name,'已经存在于数据库')
            pass
    def pop(self):
        """
                这个函数会查询队列中的所有状态为OUTSTANDING的值，
                更改状态，（query后面是查询）（update后面是更新）
                并返回_id（就是我们的股票代码），
                如果没有OUTSTANDING的值则调用repair()函数重置所有超时的状态为OUTSTANDING，
                $set是设置的意思，和MySQL的set语法一个意思
                """
        record = self.db.find_and_modify(
            query={'status':self.OUTSIANDING},#取出待爬取的股票代码
            update={'$set': {'status': self.PROCESSING, 'timestamp': datetime.now()}}
        )
        if record:
            return record['_id']
        else:
            self.repair()
            raise KeyError

    def repair(self):
        """这个函数是重置状态$lt是比较"""
        record = self.db.find_and_modify(
            query={
                'timestamp': {'$lt': datetime.now() - timedelta(seconds=self.timeout)},
                'status': {'$ne': self.COMPLETE}
            },
            update={'$set': {'status': self.OUTSIANDING}}
        )
        if record:
            print('重置股票代码状态', record['_id'])

    def status_setting(self):
        record = self.db.find({'status': self.PROCESSING})  # 找到所有状态为2的状态

        for i in record:
            id = i["_id"]
            self.db.update({'_id': id}, {'$set': {'status': self.OUTSIANDING}})  # 该状态为1，重新测试
            print('股票', id, '状态更改成功')
    def check_status(self,symbol):
        record = self.db.find_one({'_id':symbol,'status': self.COMPLETE})
        if record:
            return True
        else:
            return False

    def push_stock_comment(self,comment_id,symbol,comment,user_id,user,title,):
        '''根据评论ID进行更新，按道理评论ID是唯一的可以作为表ID，但是毕竟评论太对，直接作为ID怕重复'''
        try:
            self.db.update({'评论ID':comment_id},{'评论ID':comment_id,'股票代码':symbol,'评论内容':comment,'评论者ID':user_id,'评论标题':title},True)
            print('股票',symbol,'的一页评论内容插入成功')
        except:
            pass

    def complete(self, symbol):
        self.db.update({'_id': symbol}, {'$set': {'status': self.COMPLETE}})

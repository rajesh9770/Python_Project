import threading


import redis

import sys

import time

REDIS_IP = '35.185.229.45'

# r = redis.StrictRedis(host=REDIS_IP, port=6379, db=0)
# print ("set key1 123")
# print (r.set('key1', '123'))
# print ("get key1")
# print(r.get('key1'))
# print(r.delete('key1'))

def print_time( threadName, delay):
    count = 0
    while count < 5:
        time.sleep(delay)
        count += 1
        print "%s: %s" % ( threadName, time.ctime(time.time()) )

def addKeys(r=None, start=0, stop=100):
    #if not r : r = redis.StrictRedis(host=REDIS_IP, port=6379, db=0)
    #r.flushall()
    for i in xrange(start, stop):
        r.set('key.{}'.format(i), i)
        #print "{}-Val:{}".format(threading.currentThread().getName(), i)
        #sys.stdout.flush()
        #time.sleep(10)
    #print (r.dbsize())


def incr(r, transact,  key, start, stop, sleep):
    for i in xrange(start, stop):
        transact.incr(key)
        #print "{}-Val:{}".format(threading.currentThread().getName(), r.get("test"))
        #sys.stdout.flush()
        time.sleep(sleep)


def trasact1(r, start=0, stop=100, sleep=0):
    transact = r.pipeline(transaction=True)
    #transact.watch("test")
    transact.multi()
    #addKeys(transact, start=start, stop=stop)
    print r.get("test")
    if not r.get("test"):
        incr(r, transact, "test", start, stop, sleep)
    transact.execute()
    print r.get("test")


def test_trasaction(howMany=100):
    #r1 = redis.StrictRedis(host=REDIS_IP, port=6379, db=0)
    threads = []
    key = "test"

    try:
        for i in xrange(0, 2):
            r1 = redis.StrictRedis(host=REDIS_IP, port=6379, db=0)
            r1.delete(key)
            threads.append(threading.Thread(target=trasact1, args=(r1, 100*i, 100*i+100, 0)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

    except:
        print sys.exc_info()[0]





if __name__ == '__main__':
    for i in xrange(0, 8):
        print i
    #test_trasaction()
    #import packaging




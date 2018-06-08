import logging

import time

import ipaddress
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, ZookeeperError, NoNodeError
from kazoo.protocol.states import KazooState

ZK_HOST='35.233.230.239'
ZK_PORT1='2182'
ZK_PORT2='2183'
ZK_PORT3='2184'
CONNECT_STRING = "{}:{}".format(ZK_HOST,ZK_PORT1)

zk = None
master_id = "1"


def my_listener(state):
    if state == KazooState.LOST:
        logging.info('Connection Lost')
    elif state == KazooState.SUSPENDED:
        logging.info("Handle being disconnected from Zookeeper")
    else:
        logging.info("Handle being connected/reconnected to Zookeeper")


def open_connection():
    global zk
    zk = KazooClient(hosts=CONNECT_STRING, timeout=50)
    zk.add_listener(my_listener)
    zk.start(timeout=150)


def run_for_master():
    global zk
    try:
        result = zk.create('/master_node', value=master_id.encode('UTF-8'), ephemeral=True)
        logging.info(str(result))
        return True
    except NodeExistsError as nodeExists:
        logging.info('Node already exists')
    except ZookeeperError as zkServerError:
        logging.info('ZK server error')

    return False


def is_master():
    try:
        data, _ = zk.get('/master_node')
        logging.info(str(data))
        if data.decode('UTF-8') == master_id : return True
        else: return False
    except NoNodeError as no_node:
        logging.info('Node does not exists')
    except ZookeeperError as zkServerError:
        logging.info('ZK server error')
    return False

if __name__ == '__main__':

    logging.basicConfig(filename='zk_client.log', level=logging.DEBUG)
    try:
        open_connection()
        run_for_master()
        logging.info(is_master())
    finally:
        zk.stop()
        zk.close()



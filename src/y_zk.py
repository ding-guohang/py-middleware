# coding: utf-8
# !/usr/bin/python

"""
Author: 丁国航 meow
Email: dingguohang@kuaishou.com
Date: 2019/11/8
"""

from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch, DataWatch


class YZK(object):

    def __init__(self, zk_path):
        self.zk_path = zk_path
        self.client = KazooClient(hosts=zk_path)
        self.client.start()
        self.children_watchers = []
        self.data_watchers = []

    def add_children_watcher(self, children_path, on_change):
        watcher = ChildrenWatch(self.client, path=children_path, func=on_change)
        self.children_watchers.append(watcher)

    def add_node_watcher(self, node_path, on_change):
        watcher = DataWatch(self.client, path=node_path, func=on_change)
        self.data_watchers = watcher

    def get_watchers(self):
        print(self.children_watchers)
        print(self.data_watchers)

    def test_write(self):
        self.client.set("/y/cdn/video", b"some data input into video")
        self.client.ensure_path("/y/cdn/child_demo2")

    def stop(self):
        self.client.stop()


def on_children_change(children):
    print("children change")
    print(children)


def on_node_change(data, stat):
    print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))


if __name__ == '__main__':
    zk = YZK("127.0.0.1:2181")

    zk.add_children_watcher("/y/cdn", on_children_change)
    zk.add_node_watcher("/y/cdn/video", on_node_change)

    zk.get_watchers()

    zk.test_write()
    zk.stop()

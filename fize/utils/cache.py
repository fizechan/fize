# -*- coding: utf-8 -*-

import os
import datetime
import pickle
from functools import wraps


class Cache:
    """
    Cache缓存类
    """

    def __init__(self, path=None):
        """
        初始化
        :param path: 指定路径，不指定则默认为当前目录下的cache文件夹
        """
        if path is None:
            path = os.path.abspath(os.getcwd() + os.path.sep + "cache")
        self.__path = path
        if not os.path.isdir(path):
            os.makedirs(path)

    def set(self, key, val, duration=None, expiry_time=None):
        """
        设置一个cache
        :param key: cache键名
        :param val: cache值
        :param duration: 有效时长，单位秒。
        :param expiry_time: 过期时间，权重大于duration
        :return: void
        """
        full_path = self.__path + os.path.sep + key + ".pkl"
        if expiry_time is not None:  # expiry_time 优先处理
            pass
        elif duration is not None:
            expiry_time = datetime.datetime.now() + datetime.timedelta(seconds=duration)
        data = {
            'val': val,
            'expiry_time': expiry_time
        }
        with open(full_path, 'wb+') as store:
            # store.truncate()
            pickle.dump(data, store, protocol=2)

    def get(self, key):
        """
        获取一个cache
        :param key: cache键名
        :return: mixed
        """
        full_path = self.__path + os.path.sep + key + ".pkl"
        if os.path.isfile(full_path):
            with open(full_path, 'rb') as store:
                data = pickle.load(store)
                if data["expiry_time"] is None:  # None表示永久有效
                    return data["val"]
                else:
                    if data["expiry_time"] > datetime.datetime.now():
                        return data["val"]
                    else:
                        return None
        else:
            return None

    def has(self, key):
        """
        判断是否存在cache
        :param key: cache键名
        :return: 
        """
        return self.get(key) is not None

    def remove(self, key):
        """
        删除指定cache
        :param key: cache键名
        :return: 
        """
        full_path = self.__path + os.path.sep + key + ".pkl"
        if os.path.isfile(full_path):
            os.remove(full_path)

    def clear(self):
        """
        清空所有cache
        :return: 
        """
        import shutil
        shutil.rmtree(self.__path)


def cache_daily(func):
    """
    装饰器，用于缓存函数结果一天
    :param func: 
    :return: 
    """
    @wraps(func)
    def wrap(*args):
        cache = Cache()
        val = cache.get(func.__name__)
        if val is not None:
            return val
        else:
            result = func(*args)
            cache.set(func.__name__, result, expiry_time=datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d") + " 23:59:59", "%Y-%m-%d %H:%M:%S"))
            return result
    return wrap

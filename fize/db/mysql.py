import pymysql


class Mysql:

    def __init__(self):
        print(pymysql.VERSION)
        print(pymysql.get_client_info())

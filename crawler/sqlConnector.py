# !/usr/bin/python
# -*- coding: utf-8 -*-

import pymysql.cursors

__author__ = 'qiaoyang'
__time__ = '2021/5/23 19:15'
__project__ = 'hackthon2021'


class DisconnectSafeCursor(object):
    db = None
    cursor = None

    def __init__(self, db, cursor):
        self.db = db
        self.cursor = cursor

    def close(self):
        self.cursor.close()

    def execute(self, *args, **kwargs):
        try:
            return self.cursor.execute(*args, **kwargs)
        except pymysql.OperationalError:
            self.db.reconnect()
            self.cursor = self.db.cursor()
            return self.cursor.execute(*args, **kwargs)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


class DisconnectSafeConnection(object):
    # 'host': 'dcphackathon.mysql.database.azure.com',
    # 'user': 'yangqiao@dcphackathon',
    # 'password': 'Hackathon2021',
    # 'database': 'dcphackathonRaddit',
    # #   'client_flags': [mysql.connector.ClientFlag.SSL],
    # 'ssl_ca': '/var/wwww/html/DigiCertGlobalRootG2.crt.pem'

    def __init__(self, host, user, password, database, ssl_ca):
        self.conn = pymysql.connect(host=host, user=user, password=password, database=database, ssl_ca=ssl_ca, cursorclass=pymysql.cursors.DictCursor)

    def cursor(self):
        cur = self.conn.cursor()
        return DisconnectSafeCursor(self, cur)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

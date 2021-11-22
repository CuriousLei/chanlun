# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 16:27:23 2021

@author: leida
"""
import pymysql
import logging
class MysqlUtils():
    # 初始化方法
    def __init__(self,host,port,user,passwd,dbName,charsets):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbName = dbName
        self.charsets = charsets
        # self.db = pymysql.Connect(
        #     host=host,
        #     port=port,
        #     user=user,
        #     passwd=passwd,
        #     db=dbName,
        #     charset=charsets
        # )
        # self.cursor = self.db.cursor()
    #链接数据库
    def getCon(self):
        self.db = pymysql.Connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=self.dbName,
            charset=self.charsets
        )
        self.cursor = self.db.cursor()
    #关闭链接
    def close(self):
        self.cursor.close()
        self.db.close()
    #查询单行记录
    def get_one(self,sql):
        res = None
        try:
            self.getCon()
            self.cursor.execute(sql)
            res = self.cursor.fetchone()
        except Exception as e:
            logging.exception(e)
        return res
    #查询列表数据
    def get_all(self,sql):
        res = None
        try:
            self.getCon()
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            self.close()
        except Exception as e:
            logging.exception(e)
        return res
    #修改数据
    def __modify(self,sql):
        count = 0
        try:
            self.getCon()
            count = self.cursor.execute(sql)
            self.db.commit()
            self.close()
        except Exception as e:
            logging.exception(e)
            self.db.rollback()
        return count
    #修改数据
    def insert(self,sql):
        return self.__modify(sql)
    #修改数据
    def edit(self,sql):
        return self.__modify(sql)
    #删除数据
    def delete(self,sql):
        return self.__modify(sql)
    #更新数据
    def update(self,sql):
        return self.__modify(sql)
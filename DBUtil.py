import pymysql

# MySQL 操作类


class DBUtil:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise(NameError, "没有设置数据库信息")
        self.conn = pymysql.connect(
            host=self.host, user=self.user, password=self.pwd, database=self.db)
        cur = self.conn.cursor()
        if not cur:
            raise(NameError, "连接数据库失败")
        else:
            # print('连接成功！')
            return cur

    def ExecQuery(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        """
        cur = self.__GetConnect()
        cur.execute(sql)
        rows = cur.fetchall()
        resList = []
        for row in rows:
            resList.append(list(row))  # 每一行都转换成列表
        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecQueryVal(self, sql,val):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        """
        cur = self.__GetConnect()
        cur.execute(sql,val)
        rows = cur.fetchall()
        resList = []
        for row in rows:
            resList.append(list(row))  # 每一行都转换成列表
        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self, sql,val):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__GetConnect()
        cur.execute(sql,val)
        self.conn.commit()  # 注意如果对数据库有修改，则必须在关闭前对游标执行commit
        self.conn.close()

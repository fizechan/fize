# -*- coding: utf-8 -*-

import pymysql


class Query:

    def __init__(self, obj=None, add_quotes=True, sql="", bind=None):
        """
        初始化
        :param obj: 指定条件对象，对于某些不需要指定对象的条件，不需要传递或者传递None。
        :param add_quotes: 是否为obj添加左右引号“`”，默认True
        :param sql: 初始化的SQL查询部分构建语句，不建议外部传递该值，主要用于合并查询。
        :param bind: 初始化的参数绑定，不建议外部传递该值，主要用于合并查询。
        """
        self.__obj = None
        if obj is not None:
            if add_quotes:
                self.__obj = "`" + obj + "`"
            else:
                self.__obj = obj
        self.__sql = sql
        if bind is None:
            bind = []
        self.__bind = bind
        self.__combine_logic = "AND"

    def combine_logic(self, logic):
        """
        设置本对象当前每个条件的组合逻辑
        :param logic: 组合逻辑，未调用该方法是默认组合逻辑为“AND”
        :return: Query
        """
        self.__combine_logic = logic.upper()
        return self

    def __add_part(self, statement, bind=None):
        """
        对本对象添加一个条件块。
        注意，对象内添加条件是不添加左右括号的，如果需要添加请使用对象间条件
        :param statement: SQL语句块，支持预处理%s
        :param bind: 参数绑定数组
        """
        if self.__sql == "":
            if self.__obj is None:
                self.__sql = statement
            else:
                self.__sql = self.__obj + " " + statement
        else:
            if self.__obj is None:
                self.__sql += " " + self.__combine_logic + " " + statement
            else:
                self.__sql += " " + self.__combine_logic + " " + self.__obj + " " + statement
        if bind is not None:
            if isinstance(bind, list):
                self.__bind += bind
            else:
                self.__bind.append(bind)

    def exp(self, expression, bind=None):
        """
        使用表达式语句设置条件
        :param expression: 表达式语句
        :param bind: 参数绑定数组
        :return: Query
        """
        self.__add_part(expression, bind)
        return self

    def condition(self, judge, value, bind=None):
        """
        使用条件语句设置条件
        :param judge: 判断符
        :param value: 判断量，该值必须为标量
        :param bind: 参数绑定数组，特殊值False表示不绑定参数，None表示自动判断是否绑定
        :return: Query
        """
        if bind is False:  # False表示不需要绑定参数
            if isinstance(value, str):
                # @todo 未考虑防注入问题
                return self.exp(judge + " '" + value + "'")
            else:
                return self.exp(judge + " " + str(value))
        else:
            # @todo 针对字符串的判断是否需要更精确一些以防止过多的参数绑定
            if bind is None and isinstance(value, str):  # None表示自动判断是否绑定参数，如果此时参数为字符串形式则必须进行绑定
                return self.exp(judge + " %s", [value])
            else:
                return self.exp(judge + " " + str(value), bind)  # 对于非字符串格式的，可以不进行绑定，直接写入SQL

    def between(self, value1, value2):
        """
        使用“BETWEEN...AND”语句设置条件
        :param value1: 值1
        :param value2: 值2
        :return: Query
        """
        if isinstance(value1, str) or isinstance(value2, str):
            return self.exp("BETWEEN %s AND %s", [value1, value2])
        else:
            return self.exp("BETWEEN " + value1 + " AND " + value2)

    def egt(self, value):
        """
        使用条件“大于等于”设置条件
        :param value: 判断值
        :return:Query
        """
        return self.condition(">=", value)

    def elt(self, value):
        """
        使用条件“小于等于”设置条件
        :param value: 判断值
        :return: Query
        """
        return self.condition("<=", value)

    def eq(self, value):
        """
        使用条件“等于”设置条件
        :param value: 判断值
        :return: Query
        """
        return self.condition("=", value)

    def exists(self, expression, bind=None):
        """
        使用“EXISTS”子语句设置条件，使用EXISTS语句时不需要指定对象obj，指定时在exists方法中也没有任何作用，但可以作为对象内条件合并使用
        :param expression: EXISTS语句部分
        :param bind: 参数绑定数组
        :return: Query
        """
        if bind is False:  # exists语句的False值等同于None，做兼容性处理
            bind = None
        obj = self.__obj
        self.__obj = None
        query = self.exp("EXISTS(" + expression + ")", bind)
        self.__obj = obj
        return query

    def gt(self, value):
        """
        使用条件“大于”设置条件
        :param value: 判断值
        :return:Query
        """
        return self.condition(">", value)

    def is_in(self, values):
        """
        使用“IN”语句设置条件
        :param values: 可以传入数组(推荐)，或者IN条件对应字符串(左右括号可选)
        :return: Query
        """
        if isinstance(values, list):  # 针对values是数组的情况
            holders = ['%s' for value in values]
            return self.exp("IN(" + (str(holders).replace("'", ""))[1:-1] + ")", values)
        else:
            if values[0] == "(" and values[-1] == ")":  # 兼容性判断values是否已自带左右括号
                return self.exp("IN" + values)
            else:
                return self.exp("IN(" + values + ")")

    def is_null(self):
        """
        使用“IS NULL”语句设置条件
        :return: Query
        """
        return self.exp("IS NULL")

    def like(self, value):
        """
        使用“LIKE”语句设置条件
        :param value: LIKE字符串
        :return: Query
        """
        return self.condition("LIKE", value)

    def lt(self, value):
        """
        使用条件“小于”设置条件
        :param value: 判断值
        :return: Query
        """
        return self.condition("<", value)

    def neq(self, value):
        """
        使用条件“不等于”设置条件
        :param value: 判断值
        :return: Query
        """
        return self.condition("<>", value)

    def not_between(self, value1, value2):
        """
        使用“NOT BETWEEN...AND”语句设置条件
        :param value1: 值1
        :param value2: 值2
        :return: Query
        """
        if isinstance(value1, str) or isinstance(value2, str):
            return self.exp("NOT BETWEEN %s AND %s", [value1, value2])
        else:
            return self.exp("NOT BETWEEN " + value1 + " AND " + value2)

    def not_exists(self, expression, bind=None):
        """
        使用“NOT EXISTS”子语句设置条件，使用NOT EXISTS语句时不需要指定对象obj，指定时在not_exists方法中也没有任何作用，但可以作为对象内条件合并使用
        :param expression: EXISTS语句部分
        :param bind: 参数绑定数组
        :return: Query
        """
        if bind is False:  # exists语句的False值等同于None，做兼容性处理
            bind = None
        obj = self.__obj
        self.__obj = None
        result = self.exp("NOT EXISTS(" + expression + ")", bind)
        self.__obj = obj
        return result

    def not_in(self, values):
        """
        使用“NOT IN”语句设置条件
        :param values: 可以传入数组(推荐)，或者IN条件对应字符串(左右括号可选)
        :return: Query
        """
        if isinstance(values, list):  # 针对values是数组的情况
            holders = ['%s' for value in values]
            return self.exp("NOT IN(" + (str(holders).replace("'", ""))[1:-1] + ")", values)
        else:
            if values[0] == "(" and values[-1] == ")":  # 兼容性判断values是否已自带左右括号
                return self.exp("NOT IN" + values)
            else:
                return self.exp("NOT IN(" + values + ")")

    def not_like(self, value):
        """
        使用“NOT LIKE”语句设置条件
        :param value: LIKE字符串
        :return: Query
        """
        return self.condition("NOT LIKE", value)

    def not_null(self):
        """
        使用“IS NOT NULL”语句设置条件
        :return: Query
        """
        return self.exp("IS NOT NULL")

    def __str__(self):
        """
        返回当前的SQL语句块
        :return: str
        """
        return self.__sql

    @property
    def params(self):
        """
        获取完整的参数绑定数组
        :return: list
        """
        return self.__bind

    def __and__(self, other):
        """
        两个条件的AND组合
        :param other: 另一个条件对象
        :return: Query
        """
        sql = "(" + str(self) + ") AND (" + str(other) + ")"
        bind = self.params + other.params
        return Query(obj=None, sql=sql, bind=bind)

    def __or__(self, other):
        """
        两个条件的OR组合
        :param other: 另一个条件对象
        :return: Query
        """
        sql = "(" + str(self) + ") OR (" + str(other) + ")"
        bind = self.params + other.params
        return Query(obj=None, sql=sql, bind=bind)


class OrmMysql:
    """
    MySQL数据库ORM对象
    """

    __conn = None

    __tablePrefix = ""

    __tableName = None

    __alias = ""

    __join = ""

    __group = ""

    __order = ""

    __limit = ""

    __union = ""

    __where = ""

    __having = ""

    __whereParams = []

    __havingParams = []

    __fields = ""

    __params = []

    __sql = ""

    __last_sql = ""

    def __init__(self, host, user, password, database, port=3306, charset="utf8"):
        self.__conn = pymysql.connect(host=host, user=user, password=password, database=database, port=port, charset=charset)

    def __del__(self):
        self.__conn.close()

    @property
    def prototype(self):
        """
        返回当前使用的数据库对象原型，用于原生操作
        :return: Connection
        """
        return self.__conn

    def table(self, name, prefix=None):
        """
        指定当前操作的表，支持链式调用
        :param name: 表名
        :param prefix: 表前缀
        :return: OrmMysql
        """
        self.__tableName = name
        if prefix is not None:
            self.__tablePrefix = prefix
        else:
            self.__tablePrefix = ""
        return self

    def alias(self, alias):
        """
        对当前表设置别名,支持链式调用
        :param alias: 
        :return: OrmMysql
        """
        self.__alias = alias
        return self

    def join(self, table, on="", jointype="LEFT JOIN"):
        """
        JOIN条件,多次调用可以join多个语句,支持链式调用
        :param table: 表名，可将ON条件一起带上
        :param on: ON条件，建议ON条件单独开来
        :param jointype: JOIN形式
        :return: OrmMysql
        """
        t_str = " " + jointype + " " + table
        if on != "":
            t_str += on
        self.__join += t_str
        return self

    def left_join(self, table, on):
        """
        LEFT JOIN条件,多次调用可以join多个语句,支持链式调用
        :param table: 表名，可将ON条件一起带上
        :param on: ON条件，建议ON条件单独开来
        :return: OrmMysql
        """
        return self.join(table, on, "LEFT JOIN")

    def right_join(self, table, on):
        """
        RIGHT JOIN条件,多次调用可以join多个语句,支持链式调用
        :param table: 表名，可将ON条件一起带上
        :param on: ON条件，建议ON条件单独开来
        :return: OrmMysql
        """
        return self.join(table, on, "RIGHT JOIN")

    def group(self, fields):
        """
        GROUP语句,支持链式调用
        :param fields: 要GROUP的字段字符串或则数组
        :return: OrmMysql
        """
        if isinstance(fields, list):
            fields = ",".join(fields)
        if self.__group == "":
            self.__group = fields
        else:
            self.__group += "," + fields
        return self

    def order(self, order):
        """
        设置排序条件,支持链式调用
        :param order: 排序条件
        :return: OrmMysql
        """
        self.__order = order
        return self

    def limit(self, rows, offset=None):
        """
        设置LIMIT,支持链式调用
        :param rows: 要返回的记录数
        :param offset: 要设置的偏移量
        :return: OrmMysql
        """
        if offset is None:
            self.__limit = str(rows)
        else:
            self.__limit = str(offset) + "," + str(rows)
        return self

    def page(self, index, size=10):
        """
        使用LIMIT语句进行简易分页,支持链式调用
        :param index: 页码
        :param size: 每页记录数量，默认每页10条记录
        :return: OrmMysql
        """
        rows = size
        offset = (index - 1) * size
        return self.limit(rows, offset)

    def union(self, sql, unionall=False):
        """
        UNION语句,支持链式调用
        :param sql: 要UNION的SQL语句
        :param unionall: 是否UNION ALL，默认False
        :return: OrmMysql
        """
        if unionall:
            self.__union += " UNION ALL (" + sql + ")"
        else:
            self.__union += " UNION (" + sql + ")"
        return self

    def where(self, stat, *args):
        """
        设置WHERE语句，在一次执行中只能运行一次
        :param stat: WHERE子语句,支持%s占位符，也可以直接传入Query对象
        :param args: 预处理替换参数数组
        :return: OrmMysql
        """
        if isinstance(stat, Query):
            self.__where = str(stat)
            self.__whereParams = stat.params
        else:
            self.__where = stat
            self.__whereParams = list(args)
        return self

    def having(self, sql):
        return self

    def field(self, fields):
        """
        指定要查询的字段，支持链式调用
        :param fields: 要查询的字段组成的数组或字符串
        :return: OrmMysql
        """
        if isinstance(fields, list):
            fields = ",".join(fields)
        if fields is None:
            fields = "*"
        self.__fields = fields
        return self

    def __build_sql(self, action, datadict=None):
        """
        根据当前条件构建SQL预查询语句
        :param action: SQL语句类型
        :param datadict: 可能需要的数据词典
        :return: string 最后组装的SQL语句
        """
        if action == "DELETE":  # 删除
            sql = "DELETE FROM `" + self.__tablePrefix + self.__tableName + "`"
            self.__params = self.__whereParams + self.__havingParams
        elif action == "INSERT":  # 添加
            fields = []
            holder = []
            for key, val in datadict.items():
                fields.append(key)
                self.__params.append(val)
                holder.append("%s")
            sql = "INSERT INTO `" + self.__tablePrefix + self.__tableName + "` (`" + "`,`".join(fields) + "`) VALUES (" + ",".join(holder) + ")"
            self.__sql = sql
            return sql  # INSERT语句已完整
        elif action == "REPLACE":  # 替换
            fields = []
            holder = []
            for key, val in datadict.items():
                fields.append(key)
                self.__params.append(val)
                holder.append("%s")
            sql = "REPLACE INTO `" + self.__tablePrefix + self.__tableName + "` (`" + "`,`".join(fields) + "`) VALUES (" + ",".join(holder) + ")"
            self.__sql = sql
            return sql  # REPLACE语句已完整
        elif action == "SELECT":  # 查询
            sql = "SELECT " + self.__fields + " FROM `" + self.__tablePrefix + self.__tableName + "`"
            self.__params = self.__whereParams + self.__havingParams
        elif action == "TRUNCATE":  # 清空
            sql = "TRUNCATE TABLE `" + self.__tablePrefix + self.__tableName + "`"
            self.__sql = sql
            return sql  # TRUNCATE语句已完整
        elif action == "UPDATE":  # 更新
            parts = []
            for key, val in datadict.items():
                parts.append("`" + key + "`=%s")
                self.__params.append(val)
            sql = "UPDATE `" + self.__tablePrefix + self.__tableName + "` SET " + ",".join(parts)
            self.__params += self.__whereParams + self.__havingParams
        else:  # 仅需要支持DELETE、INSERT、REPLACE、SELECT、UPDATE，防止其他语句进入
            self.__sql = ""
            return ""
        if self.__alias != "":
            sql += " AS " + self.__alias
        if self.__join != "":
            sql += " " + self.__join
        if self.__where != "":
            sql += " WHERE " + self.__where
        if self.__group != "":
            sql += " GROUP BY " + self.__group
        if self.__having != "":
            sql += " HAVING " + self.__having
        sql += self.__union
        if self.__order != "":
            sql += " ORDER BY " + self.__order
        if self.__limit != "":
            sql += " LIMIT " + self.__limit
        self.__sql = sql
        self.__last_sql = sql
        return sql

    @property
    def last_sql(self):
        """
        最后一次运行的SQL语句
        :return: str
        """
        return self.__last_sql

    def __clear_environment(self):
        """
        清空当前条件，以便于下次查询
        :return: void
        """
        self.__alias = ""
        self.__join = ""
        self.__where = ""
        self.__group = ""
        self.__having = ""
        self.__fields = ""
        self.__order = ""
        self.__limit = ""
        self.__union = ""

        self.__sql = ""

        # self.__havingParams = []
        # self.__whereParams = []
        # self.__params = []

        self.__havingParams.clear()
        self.__whereParams.clear()
        self.__params.clear()

    def query(self, sql, params=None):
        """
        执行一个SQL语句并返回相应结果
        :param sql: SQL语句，支持%s占位符预处理
        :param params: 可选的绑定参数
        :return: mixed SELECT语句返回数组或不返回，INSERT/REPLACE返回自增ID，其余返回受影响行数
        """
        if sql[:6].upper() == "INSERT" or sql[:7].upper() == "REPLACE":
            cursor = self.__conn.cursor()
            cursor.execute(sql, params)
            self.__conn.commit()
            cursor.close()
            return cursor.lastrowid  # 返回自增ID
        elif sql[:6].upper() == "SELECT":
            cursor = self.__conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql, params)
            return cursor.fetchall()  # 返回数组
        else:
            cursor = self.__conn.cursor()
            effect_row = cursor.execute(sql, params)
            self.__conn.commit()
            cursor.close()
            return effect_row  # 返回受影响条数

    def add(self, datadict):
        """
        插入记录，返回自增ID
        :param datadict: 数据词典
        :return: 自增ID
        """
        self.__build_sql("INSERT", datadict)
        new_id = self.query(self.__sql, self.__params)
        self.__clear_environment()
        return new_id

    def replace(self, datadict):
        """
        以替换形式添加记录，返回自增ID
        :param datadict: 数据词典
        :return: 自增ID
        """
        self.__build_sql("REPLACE", datadict)
        new_id = self.query(self.__sql, self.__params)
        self.__clear_environment()
        return new_id

    def select(self, fields=None):
        """
        执行查询，返回结果记录列表
        :param fields: 要查询的字段组成的数组
        :return: 
        """
        self.field(fields)
        self.__build_sql("SELECT")
        rows = self.query(self.__sql, self.__params)
        self.__clear_environment()
        return rows

    def find(self, fields=None):
        """
        执行查询，获取单条记录
        :param fields: 指定要返回的字段
        :return: 
        """
        self.field(fields)
        self.limit(1)
        self.__build_sql("SELECT")
        rows = self.query(self.__sql, self.__params)
        self.__clear_environment()
        if len(rows) > 0:
            return rows[0]
        else:
            return None

    def delete(self):
        """
        删除记录
        :return: 返回受影响记录条数
        """
        self.__build_sql("DELETE")
        effect_row = self.query(self.__sql, self.__params)
        self.__clear_environment()
        return effect_row

    def truncate(self):
        """
        清空记录
        :return: 
        """
        self.__build_sql("TRUNCATE")
        effect_row = self.query(self.__sql)
        self.__clear_environment()

    def update(self, datadict):
        """
        更新记录
        :param datadict: 要设置的数据
        :return: 返回受影响记录条数
        """
        self.__build_sql("UPDATE", datadict)
        effect_row = self.query(self.__sql, self.__params)
        self.__clear_environment()
        return effect_row

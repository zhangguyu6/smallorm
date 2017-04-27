# -*- coding: utf-8 -*-
import sqlite3

class dbdrive:
    def __init__(self, dbname):
        self.dbconn = sqlite3.connect(dbname)

    def create(self, tableclass):
        # 通过每个class的静态属性field来获得类名
        cursor = self.dbconn.cursor()
        tablename = tableclass.fields.tablename
        fields = ",".join(field.to_sql() for field in tableclass.fields.get_all_fields())
        sql="CREATE TABLE {tablename:} ({fields:});".format(tablename=tablename, fields=fields)
        cursor.execute(sql)
        self.dbconn.commit()

    def drop(self, tableclass):
        cursor = self.dbconn.cursor()
        tablename = tableclass.fields.tablename
        cursor.execute("DROP TABLE {tablename:};".format(tablename=tablename))
        self.dbconn.commit()

    def isexisttable(self,table):
        # 用sqlite系统表判断表是否存在
        Template="SELECT name FROM sqlite_master WHERE type='table' AND name='{tablename}';"

        sql=Template.format(tablename=table.__name__)
        if self.execute(sql).fetchall():
            return True
        return False


    def commit(self):
        self.dbconn.commit()

    def execute(self, sql):
        print(sql)
        cursor = self.dbconn.cursor()
        result = cursor.execute(sql)
        return result


db = dbdrive("mysql")


class Field:
    Datatype = ""
    DataTemple = "{Datatype:}"
    name = None

    def to_sql(self):
        return "{name:} {DateTemple:}".format(name=self.name, DateTemple=self.DataTemple.format(Datatype=self.Datatype))

    def bindtotable(self, name, table):
        self.name = name
        self.table = table


class CharField(Field):
    Datatype = 'VARCHAR'
    DataTemple = "{Datatype:}(255) NOT NULL"

    def returnval(self, value):
        return "" or value


class TextField(Field):
    Datatype = 'TEXT'

    def returnval(self, value):
        return "" or value


class FloatField(Field):
    Datatype = 'REAL'
    DataTemple = "{Datatype:} NOT NULL"

    def returnval(self, value):
        return "" or value


class IntegerField(Field):
    Datatype = 'INTEGER'
    DataTemple = "{Datatype:} NOT NULL"

    def returnval(self, value):
        return "" or value


class Primary_key(IntegerField):
    DataTemple = "{Datatype:} PRIMARY KEY NOT NULL"


class Foreign_Key(IntegerField):
    DataTemple = "{Datatype:} NOT NULL REFERENCES {referencetable:}({referencename:})"

    def __init__(self, relatedfield):
        self.relatedfield = relatedfield
        self.referencetable = relatedfield.table
        self.referencename = relatedfield.name

    def to_sql(self):
        return "{name:} {DateTemple:}".format(name=self.name,
                                              DateTemple=self.DataTemple.format(
                                                  Datatype=self.Datatype,
                                                  referencetable=self.referencetable.__name__,
                                                  referencename=self.referencename))

    def bindtotable(self, name, table):
        self.name = name
        self.foreigntable = table


class Left:
    # 每个table的filed包装成一个left,将每个python-like express重载为ConditionExp
    # = | ! = | > | > = |  < | < =
    def __init__(self, name, cls, field):
        self.name = name
        self.table = cls
        self.field = field

    def __eq__(self, other):
        return ConditionExp(self, "=", self.convert(other))

    def __ne__(self, other):
        return ConditionExp(self, "!=", self.convert(other))

    def __lt__(self, other):
        return ConditionExp(self, "<", self.convert(other))

    def __le__(self, other):
        return ConditionExp(self, "<=", self.convert(other))

    def __gt__(self, other):
        return ConditionExp(self, ">", self.convert(other))

    def __ge__(self, other):
        return ConditionExp(self, ">=", self.convert(other))

    def convert(self, other):
        if isinstance(other, str):
            other = "'{}'".format(other)
            return other
        return str(other)

    def to_sql(self):
        return self.table.__name__ + "." + self.name


class ConditionExp:
    # left的表达式重载为一个ConditionExp
    # &->AND, |->OR,- ->NOT
    def __init__(self, *args):
        self.explist = []
        for arg in args:
            if not isinstance(arg, tuple):
                self.explist.append(arg)
            else:
                self.explist.extend(arg)

    def to_sql(self):
        sqllist = []
        for term in self.explist:
            if isinstance(term, Left):
                sqllist.append(term.to_sql())
            else:
                sqllist.append(term)
        return " ".join(sqllist)

    def __and__(self, other):
        return ConditionExp(*self.explist, "AND", *other.explist)

    def __or__(self, other):
        return ConditionExp(*self.explist, "OR", *other.explist)

    def __neg__(self):
        return ConditionExp("NOT", *self.explist)


# user1=Left("User.title1","User")
# user2=Left("User.title2","User")
#
# a=(user1=="123")
# b=(user2=="456")
# print(a.explist)
# print(b.explist)
# c=a|b
# c=a & c
# print(c.to_sql())
# print(((user1=="123")|(user2=="456")).to_sql())

class Query:
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.where_condition = []

    def where(self, conditionexp=None, **kwargs):
        """
        <search_condition> ::=
        { [ NOT ] <predicate> | ( <search_condition> ) }
        [ { AND | OR } [ NOT ] { <predicate> | ( <search_condition> ) } ]
        <predicate> ::=
        { expression { = | ! = | > | > = |  < | < =  } expression
        """
        # selsect
        if conditionexp:
            self.where_condition.append(conditionexp.to_sql())
        # upadate
        for col, val in kwargs.items():
            value = str(val) if not isinstance(val, str) else "'" + val + "'"
            col = getattr(self.table, col)
            self.where_condition.append((col, value))
        return self

    def complie(self):
        raise NotImplemented

    def execute(self):
        result = self.db.execute(self.complie())
        return result


class SelectQuery(Query):
    """
    SELECT select_list
    [ FROM table_source ]
    [ WHERE search_condition ]
    [ GROUP BY group_by_expression ]
    [ HAVING search_condition ]
    [ ORDER BY order_expression [ ASC | DESC ] ]
    """
    Template = "SELECT {select} " \
               "FROM {table}" \
               "{search_condition}" \
               "{group_by_expression}" \
               "{having_condition}" \
               "{order_expression};"

    def __init__(self, db, table=None, select="*"):
        self.db = db
        self.table_source = table
        self.select = select
        self.where_condition = []
        self.group_by_expression = []
        self.having_condition = []
        self.order_express = []
        self.up = False

    def group_by(self, *conditions):
        if isinstance(conditions, tuple):
            self.group_by_expression.extend([field.to_sql() for field in conditions])
        else:
            self.group_by_expression.append(conditions[0].to_sql())
        return self

    def having(self, *conditions):
        # having语句未进行运算符重载,直接输入字符串
        self.having_condition.extend(conditions)
        return self

    def order_by(self, *conditions, up=False):
        for condition in conditions:
            self.order_express.append(condition.to_sql())
        self.up = up
        return self

    def complie(self):
        if isinstance(self.select, str):
            select = self.select
        elif isinstance(self.select, list):
            select = ",".join([col.tosql() for col in self.select])
        elif isinstance(self.select, Left):
            select = self.select.to_sql()
        if not isinstance(self.table_source, list):
            table_source = self.table_source.__name__
        else:
            table_source = ",".join(table.__name__ for table in self.table_source)
        where = "AND".join(self.where_condition)
        if where:
            where = " WHERE " + where
        group = ",".join(self.group_by_expression)
        if group:
            group = " GROUP BY " + group
        having = self.having_condition[0] if self.having_condition else ""
        if having:
            having = " HAVING " + having
        order_by = ",".join(self.order_express)
        if self.up and order_by:
            order_by += " ASC"
        else:
            if order_by:
                order_by += " DESC"
        if order_by:
            order_by = " ORDER BY " + order_by

        return self.Template.format(select=select,
                                    table=table_source,
                                    search_condition=where,
                                    group_by_expression=group,
                                    having_condition=having,
                                    order_expression=order_by)

    def execute(self):
        result = self.db.execute(self.complie())
        if isinstance(self.select, str) and self.select=="*":
            namelist=self.table_source.fields.get_all_fieldname()
        elif isinstance(self.select, list):
            namelist = [col.name for col in self.select]
        elif isinstance(self.select, Left):
            namelist = [self.select.name]
        return [Result(self.table_source,rowresult,namelist) for rowresult in result.fetchall()]




class Updata(Query):
    """
    UPDATE table_name
    SET column1=value1,column2=value2,...
    WHERE some_column=some_value
    """
    Template = "UPDATE {tablename} " \
               "SET {setconditions} " \
               "{wherecondition};"

    def __init__(self, db, table=None):
        super().__init__(db, table)
        self.setconditions = []

    def set(self, **kwargs):
        for column, val in kwargs.items():
            value = str(val) if not isinstance(val, str) else "'" + val + "'"
            column = getattr(self.table, column)
            condtion = "{column}={value}".format(column=column.name, value=value)
            self.setconditions.append(condtion)
            return self

    def complie(self):
        tablename = self.table.__name__
        setconditions = ",".join(self.setconditions)
        wherecondition = ",".join("{}={}".format(pair[0].to_sql(), pair[1]) for pair in self.where_condition)
        if wherecondition:
            wherecondition = "WHERE " + wherecondition

        return self.Template.format(tablename=tablename, setconditions=setconditions, wherecondition=wherecondition)


class Delete(Query):
    """
    DELETE FROM table_name
    WHERE [condition];
    """
    Template = "DELETE FROM {tablename} " \
               "{wherecondition};"

    def __init__(self, db, table=None):
        super().__init__(db, table)

    def complie(self):
        tablename = self.table.__name__
        wherecondition = "AND".join(self.where_condition)
        if wherecondition:
            wherecondition = "WHERE " + wherecondition

        return self.Template.format(tablename=tablename, wherecondition=wherecondition)


class Insert(Query):
    """
    INSERT INTO TABLE_NAME (column1, column2, column3,...columnN)
    VALUES (value1, value2, value3,...valueN);
    """
    Template = "INSERT INTO {tablename} ({fields}) " \
               "VALUES ({values});"

    def __init__(self, db, table=None, **kwargs):
        self.db = db
        self.table = table
        self.coldict = kwargs
        self.cols = []
        self.vals = []

    def complie(self):
        for key, val in self.coldict.items():
            self.cols.append(key)
            value = str(val) if not isinstance(val, str) else "'" + val + "'"
            self.vals.append(value)
        fields = ",".join(self.cols)
        values = ",".join(self.vals)
        tablename = self.table.__name__
        return self.Template.format(tablename=tablename, fields=fields, values=values)


class Metamodel(type):
    # def __new__(cls,*args, **kwargs):
    #     cls=super().__new__(cls,*args, **kwargs)
    #     name,base,ns=args
    #
    #     class Fields:
    #         fields = {}
    #
    #         def __init__(self, tableclass):
    #             self.tableclass = tableclass
    #             self.db = tableclass.db
    #
    #         def __str__(self):
    #             return str(self.fields)
    #
    #     fields=Fields(cls)
    #     for name,field in ns.items():
    #         if isinstance(field,Field):
    #             fields.fields[name]=field
    #     cls.fields=fields
    #     return cls
    # 目的是保存table的field字段到table的field属性
    def __init__(cls, name, bases, ns, **kwargs):
        super().__init__(name, bases, ns)

        class Fields:
            def __init__(self, tableclass):
                self.tableclass = tableclass
                self.tablename = tableclass.__name__
                self.db = tableclass.db
                self.fields = {}

            def get_all_fields(self):
                return self.fieldlist

            def get_all_fieldname(self):
                return self.namelist

            def get_field(self, name):
                return self.fields[name]

            def __str__(self):
                return str([(name, field.__class__.__name__) for name, field in self.fields.items()])

        fields = Fields(cls)
        has_primarykey = False
        for name, field in ns.items():
            if isinstance(field, Field):
                if isinstance(field, Primary_key):
                    has_primarykey = False
                if isinstance(field, Foreign_Key):
                    if isinstance(field.relatedfield, Primary_key):
                        raise KeyError
                setattr(cls, name, Left(name, cls, field))
                fields.fields[name] = field
                field.bindtotable(name, cls)

        if not has_primarykey:
            id = Primary_key()
            setattr(cls, "id", Left("id", cls, id))
            fields.fields["id"] = id
            id.bindtotable("id",cls)

        cls.fields = fields
        namelist=[]
        fieldlist=[]
        for name,field in cls.fields.fields.items():
            namelist.append(name)
            fieldlist.append(field)

        cls.fields.namelist=namelist
        cls.fields.fieldlist=fieldlist


class Model(metaclass=Metamodel):
    db = db

    def __init__(self, *args, **kwargs):
        for key, item in kwargs:
            self.key = item

    def get_fields(self):
        return {key: getattr(self, key) for key in self.fields.fields}

    @classmethod
    def create(cls):
        cls.db.create(cls)

    @classmethod
    def test_create(cls):
        tablename = cls.fields.tablename
        fields = ",".join(field.to_sql() for field in cls.fields.get_all_fields())
        return "CREATE TABLE {tablename:} ({fields:});".format(tablename=tablename, fields=fields)

    @classmethod
    def drop(cls):
        cls.db.drop(cls)

    @classmethod
    def test_drop(cls):
        tablename = cls.fields.tablename
        return "DROP TABLE {tablename:};".format(tablename=tablename)

    @classmethod
    def select(cls, select="*"):
        sq = SelectQuery(db=cls.db, table=cls, select=select)
        return sq

    @classmethod
    def updata(cls):
        sq = Updata(db=cls.db, table=cls)
        return sq

    @classmethod
    def delete(cls):
        sq = Delete(db=cls.db, table=cls)
        return sq

    @classmethod
    def insert(cls, **kwargs):
        sq = Insert(db=cls.db, table=cls, **kwargs)
        return sq

class Result:
    def __init__(self,table,rawresult,namelist):
        if len(rawresult)!=len(namelist):
            raise KeyError
        self.name=table.__name__
        self.rawresult=rawresult
        self.namelist=namelist

    def __str__(self):
        return "tablename:"+self.name+" "+" ".join(["{}:{}".format(pair[0],pair[1]) for pair in  zip(self.namelist,self.rawresult)])

    def __repr__(self):
        return str(self)

# class User(Model):
#     id = Primary_key()
#     name = CharField()
#
#
# class Student(Model):
#     id = Primary_key()
#     name = CharField()
#
#
# a = User()
# print(User.test_create())
# print(User.test_drop())
# print(User.select().where(User.name < "zgy").complie())
# print(SelectQuery(db=db, table=[User, Student])
#       .where((User.name == Student.name) & (User.name < "zgy"))
#       .group_by(User.name).order_by(User.name,up=True)
#       .complie())
# print(User.updata().set(name=123).where(name="xijingping").complie())
# print(User.delete().where(name="ada").complie())
# print(User.insert(name="ass").complie())
# def test(*kwargs):
#     print(*kwargs)
#
#
# test(a=="12313")

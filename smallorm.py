# -*- coding: utf-8 -*-
import sqlite3

class dbdrive:
    def __init__(self, dbname):
        self.dbconn = sqlite3.connect(dbname)

    def create_table(self, tableclass):
        # 通过每个class的静态属性field来获得类名
        cursor = self.dbconn.cursor()
        tablename = tableclass.fields.tablename
        fields = ",".join(field.to_sql() for field in tableclass.fields.get_all_fields())
        cursor.execute("CREATE TABLE {tablename:} ({fields:});".format(tablename=tablename, fields=fields))
        self.dbconn.commit()

    def drop_table(self, tableclass):
        cursor = self.dbconn.cursor()
        tablename = tableclass.fields.tablename
        cursor.execute("DROP TABLE {tablename:};".format(tablename=tablename))
        self.dbconn.commit()

    def exexute(self, sql, commit=False):
        cursor = self.dbconn.cursor()
        result = cursor.execute(sql)
        if commit:
            self.dbconn.commit()
        return result


db = dbdrive("mysql")


class Field:
    Datatype=""
    DataTemple="{Datatype:}"
    name=None
    def to_sql(self):
        return "{name:} {DateTemple:}".format(name=self.name,DateTemple=self.DataTemple.format(Datatype=self.Datatype))

    def bindtotable(self,name,table):
        self.name=name
        self.table=table

class CharField(Field):
    Datatype = 'VARCHAR'
    DataTemple = "{Datatype:}(255) NOT NULL"
    def returnval(self,value):
        return "" or value


class TextField(Field):
    Datatype = 'TEXT'

    def returnval(self,value):
        return "" or value

class FloatField(Field):
    Datatype = 'REAL'
    DataTemple = "{Datatype:} NOT NULL"

    def returnval(self,value):
        return "" or value

class IntegerField(Field):
    Datatype = 'INTEGER'
    DataTemple = "{Datatype:} NOT NULL"

    def returnval(self,value):
        return "" or value

class Primary_key(IntegerField):
    DataTemple="{Datatype:} PRIMARY KEY NOT NULL"

class Foreign_Key(IntegerField):
    DataTemple = "{Datatype:} NOT NULL REFERENCES {referencetable:} {referencename:}"
    def __init__(self,relatedfield):
        self.relatedfield=relatedfield
        self.referencetable=relatedfield.table
        self.referencename=relatedfield.name

    def to_sql(self):
        return "{name:} {DateTemple:}".format(name=self.name,
                                              DateTemple=self.DataTemple.format(
                                                  Datatype=self.Datatype,
                                                  referencetable=self.referencetable,
                                                  referencename=self.referencename))

    def bindtotable(self,name,table):
        self.name=name
        self.foreigntable=table

class Left:
    # 每个table的filed包装成一个left,将每个python-like express重载为ConditionExp
    # = | ! = | > | > = |  < | < =
    def __init__(self,name,cls,field):
        self.name=name
        self.table=cls
        self.field=field
    def __eq__(self, other):
        return ConditionExp(self,"=",self.convert(other))

    def __ne__(self, other):
        return ConditionExp(self,"!=",self.convert(other))

    def __lt__(self,other):
        return ConditionExp(self,"<",self.convert(other))

    def __le__(self,other):
        return ConditionExp(self,"<=",self.convert(other))

    def __gt__(self,other):
        return ConditionExp(self,">",self.convert(other))

    def __ge__(self, other):
        return ConditionExp(self,">=",self.convert(other))

    def convert(self,other):
        if isinstance(other,str):
            other= "'{}'".format(other)
            return other
        return other

    def to_sql(self):
        return self.table.__name__+"."+self.name

class ConditionExp:
    # left的表达式重载为一个ConditionExp
    # &->AND, |->OR,- ->NOT
    def __init__(self,*args):
        self.explist=[]
        for arg in args:
            if not isinstance(arg,tuple):
                self.explist.append(arg)
            else:
                self.explist.extend(arg)



    def to_sql(self):
        sqllist=[]
        for term in self.explist:
            if isinstance(term,Left):
                sqllist.append(term.to_sql())
            else:
                sqllist.append(term)
        return " ".join(sqllist)

    def __and__(self, other):
        return ConditionExp(*self.explist,"AND",*other.explist)

    def __or__(self, other):
        return ConditionExp(*self.explist, "OR", *other.explist)

    def __neg__(self):
        return ConditionExp("NOT",*self.explist)


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
    def __init__(self,db,table):
        self.db=db
        self.table=table
        self.where_condition = []

    def where(self,conditionexp):
        """
        <search_condition> ::=
        { [ NOT ] <predicate> | ( <search_condition> ) }
        [ { AND | OR } [ NOT ] { <predicate> | ( <search_condition> ) } ]
        <predicate> ::=
        { expression { = | ! = | > | > = |  < | < =  } expression
        """
        self.where_condition.append(conditionexp.to_sql())
        return self



class SelectQuery(Query):
    """
    SELECT select_list
    [ FROM table_source ]
    [ WHERE search_condition ]
    [ GROUP BY group_by_expression ]
    [ HAVING search_condition ]
    [ ORDER BY order_expression [ ASC | DESC ] ]
    """
    Template="SELECT {select} " \
             "FROM {table}" \
             "{search_condition}" \
             "{group_by_expression}" \
             "{having_condition}" \
             "{order_expression};"
    def __init__(self,db,table=None,select="*"):
        self.db=db
        self.table_source=table
        self.select=select
        self.where_condition=[]
        self.group_by_expression=[]
        self.having_condition=[]
        self.order_express=[]
        self.up=False

    def group_by(self,*conditions):
        if isinstance(conditions,tuple):
            self.group_by_expression.extend([field.to_sql() for field in conditions])
        else:
            self.group_by_expression.append(conditions[0].to_sql())
        return self

    def having(self,*conditions):
        # having语句未进行运算符重载,直接输入字符串
        self.having_condition.extend(conditions)
        return self

    def order_by(self,*conditions,up=False):
        for condition in conditions:
            self.order_express.append(condition.to_sql())
        self.up=up
        return self

    def complie(self):
        if isinstance(self.select,str):
            select=self.select
        elif isinstance(self.select,list):
            select=",".join([col.tosql() for col in self.select])
        elif isinstance(self.select,Left):
            select=self.select.to_sql()
        if not isinstance(self.table_source,list):
            table_source=self.table_source.__name__
        else:
            table_source=",".join(table.__name__ for table in self.table_source)
        where="AND".join(self.where_condition)
        if where:
            where=" WHERE "+where
        group=",".join(self.group_by_expression)
        if group:
            group=" GROUP BY "+group
        having=self.having_condition[0] if self.having_condition else ""
        if having:
            having=" HAVING "+having
        order_by=",".join(self.order_express)
        if self.up and order_by:
            order_by+=" ASC"
        else:
            if order_by:
                order_by+=" DESC"
        if order_by:
            order_by=" ORDER BY "+order_by

        return self.Template.format(select=select,
                                    table=table_source,
                                    search_condition=where,
                                    group_by_expression=group,
                                    having_condition=having,
                                    order_expression=order_by)






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
                return self.fields.values()

            def get_field(self,name):
                return self.fields[name]

            def __str__(self):
                return str([(name,field.__class__.__name__) for name,field in self.fields.items()])

        fields = Fields(cls)
        has_primarykey=False
        for name, field in ns.items():
            if isinstance(field, Field):
                if isinstance(field,Primary_key):
                    has_primarykey=False
                if isinstance(field, Foreign_Key):
                    if isinstance(field.relatedfield,Primary_key):
                        raise KeyError
                setattr(cls,name,Left(name,cls,field))
                fields.fields[name] = field
                field.bindtotable(name,cls)


        if not has_primarykey:
            id =Primary_key()
            setattr(cls, "id", Left("id", cls,id))
            fields.fields["id"]=id

        cls.fields = fields


class Model(metaclass=Metamodel):
    db = db

    def __init__(self, *args, **kwargs):
        for key, item in kwargs:
            self.key = item

    def get_fields(self):
        return {key: getattr(self, key) for key in self.fields.fields}

    @classmethod
    def create_table(cls):
        cls.db.create_table(cls)

    @classmethod
    def test_create_table(cls):
        tablename = cls.fields.tablename
        fields = ",".join(field.to_sql() for field in cls.fields.get_all_fields())
        return "CREATE TABLE {tablename:} ({fields:});".format(tablename=tablename, fields=fields)

    @classmethod
    def drop_table(cls):
        cls.db.drop_table(cls)

    @classmethod
    def test_drop_table(cls):
        tablename = cls.fields.tablename
        return "DROP TABLE {tablename:};".format(tablename=tablename)

    @classmethod
    def select(cls,select="*"):
        sq=SelectQuery(db=cls.db,table=cls,select=select)
        return sq


class User(Model):
    id=Primary_key()
    name=CharField()

class Student(Model):
    id=Primary_key()
    name=CharField()


a=User()
print(User.test_create_table())
print(User.test_drop_table())
print(User.select().where(User.name=="zgy").complie())
print(SelectQuery(db=db,table=[User,Student]).where(User.name==Student.name).group_by(User.name).order_by(User.name,up=True).complie())
# def test(*kwargs):
#     print(*kwargs)
#
#
# test(a=="12313")



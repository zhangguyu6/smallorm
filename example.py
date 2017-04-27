#-*- coding: utf-8 -*-
from smallorm import Model,CharField,IntegerField,Primary_key,Foreign_Key,dbdrive


db=dbdrive("classinfo.sqlite")

class Course(Model):
    db=db
    id=Primary_key()
    classname=CharField()


class Student(Model):
    db=db
    studentname=CharField()
    classid=Foreign_Key(Course.id)
    score=IntegerField()

if db.isexisttable(Course):
    Course.drop_table()
if db.isexisttable(Student):
    Student.drop_table()
Course.create_table()
Student.create_table()

Course.insert(id=1,classname="cs001").execute()
Course.insert(id=2,classname="cs002").execute()
Course.insert(id=3,classname="cs003").execute()
Course.insert(id=4,classname="cs004").execute()
Course.insert(id=5,classname="cs005").execute()

Student.insert(studentname="tom",classid=1,score=80).execute()
Student.insert(studentname="jerry",classid=1,score=90).execute()
Student.insert(studentname="marx",classid=1,score=65).execute()

Course.select(Course.classname).where(Course.classname=="cs002").execute()
# >>> [tablename:Course classname:cs002]

Course.select(Course.classname).where(Course.id>1).execute()
# >>> [tablename:Course classname:cs002, tablename:Course classname:cs003, tablename:Course classname:cs004, tablename:Course classname:cs005]

Student.select().order_by(Student.score,up=True).execute()
# >>> [tablename:Student studentname:marx id:3 classid:1 score:65, tablename:Student studentname:tom id:1 classid:1 score:80, tablename:Student studentname:jerry id:2 classid:1 score:90]

Student.delete().where(Student.score<70).execute()
Student.select().order_by(Student.score,up=True).execute()
# >>> [tablename:Student studentname:tom classid:1 id:1 score:80, tablename:Student studentname:jerry classid:1 id:2 score:90]

Student.updata().set(classid=2).where(classid=1).execute()
Student.select().order_by(Student.score,up=True).execute()
# >>> [tablename:Student classid:2 score:80 studentname:tom id:1, tablename:Student classid:2 score:90 studentname:jerry id:2]
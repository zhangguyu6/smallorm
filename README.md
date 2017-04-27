### a simple orm for python

--------------------------

#### quick start

##### connect to sqlite

```python
from smallorm import  Model,CharField,IntegerField,Primary_key,Foreign_Key,dbdrive

db=dbdrive("classinfo.sqlite")
```

##### create table

```python
class Course(Model):
    db=db
    id=Primary_key()
    classname=CharField()
 
if db.isexisttable(Course):
    Course.drop() 
Course.create()
```

##### drop table

```python
Course.drop()
```

##### Insert

```python
Course.insert(id=1,classname="cs001").execute()
Course.insert(id=2,classname="cs002").execute()
```

##### delete

```python
Course.delete().where(classname="cs001").exexute()
```

##### select

you can use python-like express in wherecondition

```python
Course.select(Course.classname).where(Course.id>1).execute()
>>> [tablename:Course classname:cs002]
```

##### update

```python
course.updata().set(classname="cs003").where(classname="cs002").execute()
```


import datetime
from functools import wraps

from sqlalchemy import create_engine, Column, String, Integer, DATE, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

conn_url = 'mysql://root:123456@127.0.0.1:3306/test_db?charset=utf8'
engine = create_engine(conn_url, encoding='utf-8', echo=True)
Base = declarative_base(bind=engine)


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), nullable=False)
    password = Column(String(64), nullable=False)
    date = Column(DATE)

    def __repr__(self):
        return u'<%s:%d,%s,%s,%s>' % (self.__class__.__name__, self.id, self.username, self.password, self.date)


class Address(Base):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True, autoincrement=True)
    address = Column(String(64), nullable=False)

    def __repr__(self):
        return u'<%s:%d,%s>' % (self.__class__.__name__, self.id, self.address)


class Class(Base):
    __tablename__ = 'Class'
    class_number = Column(Integer, primary_key=True, autoincrement=True)
    class_name = Column(String(20), unique=True)


class Student(Base):
    __tablename__ = 'Student'
    student_number = Column(Integer, primary_key=True, autoincrement=True)
    student_name = Column(String(20), unique=True)
    class_number = Column(Integer, ForeignKey(Class.class_number, ondelete='CASCADE'), onupdate='CASCADE')


def wrapper_conn(func):
    @wraps(func)
    def _wrapper(*args, **kwargs):
        from sqlalchemy.orm import sessionmaker
        conn_pool = sessionmaker(bind=engine)
        conn = conn_pool()
        data = func(conn, *args, **kwargs)
        conn.close()
        return data

    return _wrapper


# insert
@wrapper_conn
def insert_user(conn, user):
    conn.add(user)
    conn.commit()
    conn.refresh(user)
    return user


@wrapper_conn
def insert_many_users(conn, users=None):
    if users is None:
        users = []
    conn.add_all(users)
    conn.commit()
    [conn.refresh(user) for user in users]
    return users


# query
@wrapper_conn
def query_all(conn, classname):
    users = conn.query(classname).all()
    return users


@wrapper_conn
def order_by(conn, classname):
    users = conn.query(classname).order_by(classname.id.desc()).all()
    # users = conn.query(classname).order_by(classname.id.asc()).all()
    return users


@wrapper_conn
def count(conn, classname):
    c = conn.query(classname).count()
    return c


@wrapper_conn
def page(conn, classname, num, size=5):
    data = conn.query(classname).offset((num - 1) * size).limit(size).all()
    return data


@wrapper_conn
def query_by_pk(conn, classname, pk):
    user = conn.query(classname).get(pk)
    return user


# query by parameters
@wrapper_conn
def query_user_param(conn, classname, name):
    users = conn.query(classname).filter(classname.username == name).all()
    return users


@wrapper_conn
def and_query(conn, classname, username, password):
    from sqlalchemy import and_
    users = conn.query(classname).filter(and_(classname.username == username, classname.password == password)).all()
    return users


@wrapper_conn
def group_by_name_query(conn, classname):
    from sqlalchemy.sql.functions import func
    data = conn.query(func.count(classname.id), classname.username).group_by(classname.username).all()
    return data


# part query
@wrapper_conn
def part_query(conn, classname):
    data = conn.query(classname.id.label(u'id'), classname.username.label(u'name')).filter(
        classname.username == 'zhangsan').all()
    return data


# delete
@wrapper_conn
def delete_by_pk(conn, classname, pk):
    conn.query(classname).filter(classname.id == pk).delete()
    conn.commit()


# update
@wrapper_conn
def update_user(conn, user):
    conn.add(user)
    conn.commit()


def update_name_by_pk(classname, pk, name):
    user = query_by_pk(classname, pk)
    user.username = name
    update_user(user)


if __name__ == '__main__':
    Base.metadata.create_all()
    user1 = User(username='lisi', password='123', date=datetime.datetime.today())
    user2 = User(username='wangwu', password='123', date=datetime.datetime.today())
    user3 = User(username='zhaoliu', password='123', date=datetime.datetime.today())
    user4 = User(username='chenyang', password='123', date=datetime.datetime.today())
    user13 = User(id=13, username='chenyang', password='123', date=datetime.datetime.today())
    address1 = Address(address='shanghai')
    list1 = [user1, user2, user3]
    list2 = [user4, address1]

    # insert some users
    # insert_user(user4)
    # insert_many_users(list2)

    # query a table
    # print query_all(User)
    # print query_all(Address)
    # print order_by(User)
    # print count(User)
    # print page(User, 3)
    # print query_by_pk(User, 13)
    # print query_user_param(User, 'lisi')
    # print and_query(User, 'zhangsan', 123)
    # print group_by_name_query(User)
    # print part_query(User)

    # delete a user
    # delete_by_pk(User, 13)

    # update
    # update_name_by_pk(User, 1, 'zhangjie')

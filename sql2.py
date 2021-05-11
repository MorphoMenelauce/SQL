import datetime

from sqlalchemy import create_engine, Column, String, Integer, DATE, ForeignKey, MetaData, Table, select
from sqlalchemy.ext.declarative import declarative_base

conn_url = 'mysql://root:123456@127.0.0.1:3306/test_db?charset=utf8'
engine = create_engine(conn_url, encoding='utf-8', echo=True)
Base = declarative_base(bind=engine)

metadata = MetaData()
users = Table('b_users', metadata,
              Column('id', Integer, primary_key=True),
              Column('username', String(20), nullable=False),
              Column('password', String(20)),
              Column('date', DATE)
              )
address = Table('b_address', metadata,
                Column('id', Integer, primary_key=True),
                Column('address', String(20)),
                )

# def wrapper_conn(func):
#     @wraps(func)
#     def _wrapper(*args, **kwargs):
#         from sqlalchemy.orm import sessionmaker
#         conn_pool = sessionmaker(bind=engine)
#         conn = conn_pool()
#         data = func(conn, *args, **kwargs)
#         conn.close()
#         return data


if __name__ == '__main__':
    metadata.create_all(engine)
    conn = engine.connect()
    # ins = users.insert().values(username='shangsan', password='654631', date=datetime.datetime.today())
    # result = conn.execute(users.insert(), username='wendy', password='987315')
    # ins = address.insert().values(address='shanghai')
    # conn.execute(ins)
    s = select([users, address]).where(users.c.id == address.c.id)
    result = conn.execute(s)
    for row in result:
        print(row)

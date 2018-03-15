import psycopg2
from recommender import config

from flask import g, has_app_context

_database = None

def connect():
    return psycopg2.connect(
        host=config.DB.host,
        database=config.DB.name,
        user=config.DB.user,
        password=config.DB.password,
        connect_timeout=120
    )


def connection():
    global _database
    if has_app_context():
        print('Using APP context')
        db = getattr(g, '_database', None)
        if db is None:
            print('Create new connection')
            db = g._database = connect()
        print(type(db))
        return db
    else:
        if _database is None:
            print('Using global variable')
            _database = connect()
        return _database

def close():
    print('Closing DB')
    if has_app_context():
        db = getattr(g, '_database', None)
        if db is not None:
            print('In APP context')
            db.close()
    else:
        if _database is not None:
            print('In global variable')
            _database.close()

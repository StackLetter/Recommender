import psycopg2

from recommender import config, models

psql = psycopg2.connect(
    host=config.DB.host,
    database=config.DB.name,
    user=config.DB.user,
    password=config.DB.password,
)

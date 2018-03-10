#!env/bin/python

from recommender.profiles import UserProfile
from recommender import psql, config

with psql:
    cur = psql.cursor()
    cur.execute('SELECT id FROM users WHERE account_id IS NOT NULL AND site_id = %s', (config.site_id,))
    for uid in cur:
        user = UserProfile.load(uid[0])
        user.retrain()
        user.save()


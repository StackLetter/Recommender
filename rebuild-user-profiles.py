#!env/bin/python

from recommender.profiles import UserProfile
from recommender import db, config

with db.connection() as conn:
    cur = conn.cursor()
    cur.execute('SELECT id FROM users WHERE account_id IS NOT NULL AND site_id = %s', (config.site_id,))
    for uid in cur:
        user = UserProfile.load(uid[0])
        print(user.id)
        user.retrain()
        user.save()



db.close()

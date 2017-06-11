# encoding: utf-8

from datetime import datetime
import os
import time
from urllib.parse import urljoin
import requests
import schedule


NOTIFY_TIME = '17:00'
user_api = os.environ['UDB_URL']
user_token = os.environ['UDB_TOKEN']
engine = os.environ['ENGINE_URL']


def main():
    refresh_schedule()
    while True:
        schedule.run_pending()
        time.sleep(5)


def refresh_schedule():

    schedule.clear('refresh')
    schedule.every().day.at('03:00').do(refresh_schedule)

    # update DB


    # for every user
    for u in get_all_user_config():
        system_time = get_system_nofity_time(u['undisturbed_start'], u['undisturbed_end'])
        # is location set
        if u['latitude']:
            # add user schdule
            schedule.every().day.at(u['active_notify']).do(notify, u['id'], 'user')
            # add system schedule
            schedule.every().day.at(system_time).do(notify, u['id'], 'system')
        else:
            schedule.every().wednesday.at(system_time).do(broadcast, u['id'])
            schedule.every().saturday.at(system_time).do(broadcast, u['id'])

    print(schedule.default_scheduler.jobs)


def notify(uid, category):
    user_scheduled = 1 if category == 'user' else 0
    url = urljoin(engine, '/notify_interest/{}?user_scheduled={}'.format(uid, user_scheduled))
    print (url)
    requests.post(url)


def broadcast(uid):
    config = get_user_config()
    url = urljoin(engine, '/notify_all/{}'.format(uid))
    print (url)
    requests.post(url)


def get_system_nofity_time(start, end):
    form = '%H:%M'
    system = datetime.strptime(NOTIFY_TIME, form)
    start = datetime.strptime(start, form)
    end = datetime.strptime(end, form)

    if start < end:
        between = start < system and system < end
    else:
        between = system > start or system < end
    result = system if not between else start
    return result.strftime(form)


def get_all_user_config():
    query_ids = urljoin(user_api, '/user/0?userToken=') + user_token
    print(query_ids)
    all_users = requests.get(query_ids).json()
    print(all_users)
    for user in all_users:
        yield get_user_config(user)


def get_user_config(uid):
    query_config = urljoin(user_api, '/user/{}?userToken='.format(uid)) + user_token
    print(query_config)
    return requests.get(query_config).json()


if __name__ == '__main__':
    main()

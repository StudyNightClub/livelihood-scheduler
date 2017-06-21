# encoding: utf-8

from datetime import datetime
import logging
import os
import time
from urllib.parse import urljoin
import requests
import schedule

from slack import send_to_slack

NOTIFY_TIME = '17:00'
TIME_FORMAT = '%H:%M'
user_api = os.environ['UDB_URL']
user_token = os.environ['UDB_TOKEN']
engine = os.environ['ENGINE_URL']


def main():
    logging.basicConfig(filename='scheduler.log', level=logging.INFO)
    send_to_slack('Starting scheduler...')
    refresh_schedule()
    schedule.every(30).seconds.do(refresh_schedule)
    schedule.every(60).minutes.do(heartbeat)
    while True:
        schedule.run_pending()
        time.sleep(5)


def refresh_schedule():
    try:
        schedule.clear('user')
        # for every user
        for u in get_all_user_config():
            system_time = get_system_nofity_time(u['undisturbed_start'], u['undisturbed_end'])
            # is location set
            if u['latitude']:
                # add user schdule
                user_schedule = get_user_time(u)
                if user_schedule:
                    schedule.every().day.at(user_schedule).do(notify, u['id'], 'user').tag('user')
                # add system schedule
                schedule.every().day.at(system_time).do(notify, u['id'], 'system').tag('user')
            else:
                schedule.every().wednesday.at(system_time).do(broadcast, u['id']).tag('user')
                schedule.every().saturday.at(system_time).do(broadcast, u['id']).tag('user')

        logging.info(schedule.default_scheduler.jobs)
    except Exception as e:
        logging.fatal(e)


def notify(uid, category):
    user_scheduled = 1 if category == 'user' else 0
    url = urljoin(engine, '/notify_interest/{}?user_scheduled={}'.format(uid, user_scheduled))
    logging.info('POST ' + url)
    send_to_slack('Notifying user {}\n> POST {}'.format(uid, url))
    requests.post(url)


def broadcast(uid):
    config = get_user_config(uid)
    url = urljoin(engine, '/notify_all/{}'.format(uid))
    logging.info('POST ' + url)
    send_to_slack('Broadcasting to user {}\n> POST {}'.format(uid, url))
    requests.post(url)


def get_system_nofity_time(start, end):
    if not start or not end:
        return NOTIFY_TIME

    system = datetime.strptime(NOTIFY_TIME, TIME_FORMAT)
    try:
        start = datetime.strptime(start, TIME_FORMAT)
        end = datetime.strptime(end, TIME_FORMAT)
    except ValueError as e:
        logging.warn('Unable to parse time: {}'.format(e))
        return NOTIFY_TIME

    if start < end:
        between = start < system and system < end
    else:
        between = system > start or system < end
    result = system if not between else start
    return result.strftime(TIME_FORMAT)


def get_all_user_config():
    query_ids = urljoin(user_api, '/user/0?userToken=') + user_token
    logging.info(query_ids)
    all_users = requests.get(query_ids).json()
    logging.info(all_users)
    for user in all_users:
        config = get_user_config(user)
        if config:
            yield config
        else:
            logging.error('Unable to get user config for ' + user)
            send_to_slack('*Unable to get user config for ' + user + '*')


def get_user_config(uid):
    query_config = urljoin(user_api, '/user/{}?userToken='.format(uid)) + user_token
    logging.info(query_config)
    return requests.get(query_config).json()


def get_user_time(user):
    t = user['active_notify']
    try:
        datetime.strptime(t, TIME_FORMAT)
        return t
    except ValueError:
        logging.error('Illegal active notify time format: {}'.format(t))
        send_to_slack('*Illegal active notify time format: {}*'.format(t))
        return None

def heartbeat():
    send_to_slack('The scheduler is still alive')


if __name__ == '__main__':
    main()

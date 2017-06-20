# encoding: utf-8

import os
import requests

SLACK = os.environ['SLACK_WEBHOOK']

def send_to_slack(text):
    requests.post(SLACK, json={'text': text})

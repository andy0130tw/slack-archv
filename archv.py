from pprint import PrettyPrinter

# import sqlite3 as sqlite
import slacker
import peewee

import settings
import models as m

token = settings.token
slack = slacker.Slacker(token)
pp = PrettyPrinter(indent=2).pprint

def assert_auth():
    try:
        return slack.auth.test().body
    except slacker.Error as err:
        print('Auth failed: ' + str(err))

def init():
    with m.db.atomic():
        m.init_models()
        # Add Slackbot to user list
        try:
            m.User.api(slack.users.info(user='USLACKBOT').body['user'])
        except peewee.IntegrityError:
            pass

def main():
    print('Fetching Authentication info...')
    auth_resp = assert_auth()
    if auth_resp:
        pp(auth_resp)
    else:
        return

    print('Initializating database...')
    init()

    print('Fetching Channel List...')
    channel_list = slack.channels.list().body['channels']
    with m.db.atomic():
        m.Channel.delete().execute()
        for channel in channel_list:
            m.Channel.api(channel)

main()

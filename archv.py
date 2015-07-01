from pprint import PrettyPrinter

# import sqlite3 as sqlite
import slacker
import peewee

import settings
import models as m

token = settings.token
slack = slacker.Slacker(token)
pp = PrettyPrinter(indent=2)

def assert_auth():
    try:
        return slack.auth.test().body
    except slacker.Error as err:
        print('Auth failed: ' + str(err))

def fetch_user_list():
    ''' This is a method to fetch user list.
        Updating if finding the user in the Database.
        Implemented in dirty ways '''
    with m.db.atomic():
        usrlist = slack.users.list().body['members']
        for usr in usrlist:
            try:
                insta = m.User.get(m.User.id == usr['id'])
                print('User {} found. Updating....'.format(insta.name))
                insta.delete_instance()
            except m.User.DoesNotExist:
                pass
            m.User.api(slack.users.info(usr['id']).body['user'])

def fetch_channel_list():
    ''' This is a method updating channel list.
    '''
    chanlist = slack.channels.list().body['channels']
    with m.db.atomic():
        for chan in chanlist:
            try:
                insta = m.Channel.get(m.Channel.id == chan['id'])
                print('Channel {} found. Updating...'.format(insta.name))
            except m.Channel.DoesNotExist:
                insta = m.Channel.create(**chan)
            insta.update_with_raw(raw = chan)

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
        pp.pprint(auth_resp)
    else:
        return

    print('Initializating database...')
    init()
#    print('Fetching User list...')
#    fetch_user_list()
    print('Fetching Channel list...')
    fetch_channel_list()

def test():
    with m.db.atomic():
        usrlist = m.User.select()

    print('Total # of User:', usrlist.count())
    for usr in usrlist:
            print(usr.name)
    with m.db.atomic():
        chanlist = m.Channel.select()
    print('Total # of channels:', chanlist.count())
    for chan in chanlist:
            print(chan.name)

if __name__ == '__main__':
#    main()
    fetch_channel_list()
    test()

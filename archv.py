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

            # Updating linking of Users and Channels.

            for usr in chan['members']:
                try:
                    usrref = m.User.get(m.User.id == usr)
                except m.User.DoesNotExist:
                    m.User.api(slack.users.info(usr).body['user'])

                link, created = m.ChannelUser.get_or_create(channel = insta, user = usrref)

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

    print('Initializing database...')
    init()

    print('Inserting team metadata...')
    del auth_resp['ok']
    # todo: warn if user use a different database to backup
    with m.db.atomic():
        for prop in auth_resp:
            m.Information.create(key=prop, value=auth_resp[prop]).save()

    print('Fetching User list...')
    fetch_user_list()
    print('Fetching Channel list...')
    fetch_channel_list()

def test():
    with m.db.atomic():
        usrlist = m.User.select()
        print('Total # of User:', usrlist.count())
        for usr in usrlist:
                print(usr.name)

        chanlist = m.Channel.select()
        print('Total # of channels:', chanlist.count())
        for chan in chanlist:
            print(chan.name, ':', chan.creator.name)

if __name__ == '__main__':
    main()
    # test()

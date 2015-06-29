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
    print('Fetching User list...')
    fetch_user_list()

def test():
    sp = m.User.get(m.User.name == 'daydreamer')
    print(sp.email)

main()
test()

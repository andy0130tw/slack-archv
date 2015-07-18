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
        Clear the list first. '''
    with m.db.atomic():
        usrlist = slack.users.list().body['members']
        m.User.delete().execute()
        m.User.api_insert_many(usrlist).execute()

def fetch_channel_list():
    ''' This is a method updating channel list. '''
    chanlist = slack.channels.list().body['channels']
    with m.db.atomic():
        m.Channel.delete().execute()
        m.Channel.api_insert_many(chanlist).execute()

def fetch_channel_message(channel):
    cnt = 0
    has_more = True
    # todo: try to get ts_latest from db
    result = (m.Message.select(m.Message.ts)
        .where(m.Message.channel == channel)
        .order_by(m.Message.ts.desc())
        .first())

    ts_latest = None
    ts_oldest = result.ts if result else None

    with m.db.atomic():
        while has_more:

            resp = slack.channels.history(
                oldest=ts_oldest,
                latest=ts_latest,
                count=1000,
                channel=channel.id
            ).body

            msglist = resp['messages']
            msglen = len(msglist)
            cnt += msglen

            # add channel information
            for msg in msglist:
                msg['channel'] = channel
                # create files/attachments along the message
                if 'file' in msg:
                    msgfile = m.File.api(msg['file'], True)
                    msg['_file'] = msgfile
                    del msg['file']
                if 'attachments' in msg:
                    msgatt = m.Attachment.api(msg['attachments'][0], True)
                    del msg['attachments']

            # don't insert all at once
            # or it will raise `peewee.OperationalError: too many SQL variables`
            insert_limit = 50
            for idx in range(0, msglen, insert_limit):
                m.Message.api_insert_many(msglist[idx:idx+insert_limit]).execute()

            if msglen:
                # the list is always sorted by ts desc
                ts_latest = msglist[-1]['ts']

            has_more = resp['has_more']

    return cnt

def fetch_file_comment(Fid):
    # Warning: This thing is pretty dangerous, bug exists.
    cmlist = slack.files.info(Fid).body['comments']
    with m.db.atomic():
        m.FileComment.api_insert_many(cmlist, Fid).execute()
    print('Fetched {:>4} messages from file {}'.format(len(cmlist), Fid))


def fetch_all_file_comment():
    l = []
    for f in m.File.select().iterator():
        l.append(f.id)

    for fid in l:
        fetch_file_comment(fid)

def fetch_all_channel_message():
    lst = []
    for chan in m.Channel.select().iterator():
        # print(chan.name)
        lst.append(chan)

    for chan in lst:
        cnt = fetch_channel_message(chan)
        print('Fetched {:>4} messages from #{}'.format(cnt, chan.name))


def init():
    with m.db.atomic():
        m.init_models()
        # Add Slackbot to user list
        try:
            slackbot = slack.users.info(user='USLACKBOT').body['user']
            m.User.api(slackbot, True)
        except peewee.IntegrityError:
            pass

def main():
    print('Fetching Authentication info...')
    auth_resp = assert_auth()
    pp(auth_resp)

    print('Initializing database...')
    init()

    print('Inserting team metadata...')
    del auth_resp['ok']
    # todo: warn if user use a different database to backup
    with m.db.atomic():
        for prop in auth_resp:
            meta, _ = m.Information.get_or_create(key=prop, value=auth_resp[prop])
            if prop == 'team_id' and meta.value != auth_resp['team_id']:
                print(' Warning: Team ID is inconsistent.')

    print('Fetching User list...')
    fetch_user_list()
    print('Fetching Channel list...')
    fetch_channel_list()
    print('Fetching all messages from channels...')
    fetch_all_channel_message()
    print('Fetching all comments from files...')
    fetch_all_file_comment()

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

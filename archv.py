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
    usrlist = slack.users.list().body['members']
    with m.db.atomic():
        m.User.delete().execute()
        m.User.api_bulk_insert(usrlist)

def fetch_channel_list():
    ''' This is a method updating channel list. '''
    chanlist = slack.channels.list().body['channels']
    with m.db.atomic():
        m.Channel.delete().execute()
        m.ChannelUser.delete().execute()
        m.Channel.api_bulk_insert(chanlist)

def process_message(msg):
    ''' This is a method modifying a message before insertion.
        Create models of its type and get ids,
        then prefix the original parameters with an underscore. '''
    # create files/attachments along the message
    if 'file' in msg:
        msgfile = m.File.api(msg['file'], True)
        msg['_file'] = msgfile
        # create file comments along the message
        subtype = msg.get('subtype', '')
        comment = None
        if subtype == 'file_share':
            if 'initial_comment' in msg['file']:
                comment = msg['file']['initial_comment']
                # update the field using simply id
                msgfile.initial_comment = comment['id']
                msgfile.save()
        elif subtype == 'file_comment':
            comment = msg['comment']
        if comment is not None:
            # comment is deleted upon message model creation
            # so modify without cloning one
            comment['_file'] = msgfile
            m.FileComment.api(comment, True)
        del msg['file']

    if 'attachments' in msg:
        # we store only the index of the first att. and assume that indexes of att. are always in series.
        for att in msg['attachments']:
            msgatt = m.Attachment.api(att, True)
            if '_attachment' not in msg:
                msg['_attachment'] = msgatt
        del msg['attachments']

    if 'reactions' in msg:
        for r in msg['reactions']:
            m.Reaction.api_bulk_insert(
            [ { 'message': float(msg['ts']), 'reaction': r['name'], 'user': u } for u in r['users'] ]
            )
        del msg['reactions']
    return msg

def fetch_channel_message(channel):
    cnt = 0
    has_more = True

    result = (m.Message.select(m.Message.ts)
        .where(m.Message.channel == channel)
        .order_by(m.Message.ts.desc())
        .first())

    ts_latest = None
    ts_oldest = '{:.6f}'.format(result.ts) if result else None

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

            for msg in msglist:
                # add channel information
                msg['channel'] = channel
                process_message(msg)

            m.Message.api_bulk_insert(msglist)

            if msglen:
                # the list is always sorted by ts desc
                ts_latest = msglist[-1]['ts']

            has_more = resp['has_more']

    return cnt

def fetch_all_channel_message():
    lst = []
    for chan in m.Channel.select().iterator():
        lst.append(chan)

    for chan in lst:
        cnt = fetch_channel_message(chan)
        print('Fetched {:>4} messages from #{}'.format(cnt, chan.name))


def init():
    with m.db.atomic():
        m.init_models()
        # Add version info
        m.Information.create_or_get(key='__version', value='1.0.0')
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

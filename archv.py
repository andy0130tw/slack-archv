from pprint import PrettyPrinter

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

def fetch_emoji_list():
    emolist = slack.emoji.list().body['emoji']
    with m.db.atomic():
        m.Emoji.delete().execute()
        m.Emoji.api_bulk_insert(list(emolist.items()))

def process_message(msg):
    ''' This is a method modifying a message before insertion.
        Create models of its type and get ids,
        then prefix the original parameters with an underscore. '''
    # create files/attachments along the message
    if 'file' in msg:
        if 'reactions' in msg['file']:
            insert_reactions(msg['file']['reactions'], 'file', msg['file']['id'])
            del msg['file']['reactions']

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
            if 'reactions' in comment:
                insert_reactions(comment['reactions'], 'file_comment', comment['id'])
                del comment['reactions']

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

    return msg

def insert_reactions(reactions, item_type='message', item_id=None, channel=None):
    for r in reactions:
        # clear original reactions at first
        query = (m.Reaction.delete()
            .where(m.Reaction.item_type == item_type
                and m.Reaction.item_id == item_id))

        if item_type == 'message':
            query = query.where(m.Reaction.channel == channel)

        query.execute()

        # according to documentation, only a limited number of shown users is presented.
        # requiring one more query to ensure.
        # for now, we only show a warning.
        m.Reaction.api_bulk_insert([
            {
                'item_type': item_type,
                'channel': channel,
                'item_id': item_id,
                'reaction': r['name'],
                'user': u
            } for u in r['users']
        ])
        if r['count'] != len(r['users']):
            print('Warning: the reaction of channel #{} at ts={} is not saved completely.'.format(channel.id, ts))

def fetch_channel_message(channel):
    cnt = 0
    has_more = True

    result = (m.Message.select(m.Message.ts)
        .where(m.Message.channel == channel)
        .order_by(m.Message.ts.desc())
        .first())

    ts_latest = 1e10
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
                if 'reactions' in msg:
                    insert_reactions(msg['reactions'], 'message', msg['ts'], channel)
                    del msg['reactions']

            m.Message.api_bulk_insert(msglist)

            if msglen:
                # the list is always sorted by ts desc
                ts_latest = msglist[-1]['ts']

            has_more = resp['has_more']

    return cnt

def fetch_channel_message_diff(channel):
    ''' FIXME: dirty workaround. DRY solution needed.
        todo: update attachments as well. '''
    list_mod = []
    has_more = True

    result = (m.Message.select(m.Message.ts)
        .where(m.Message.channel == channel)
        .order_by(m.Message.ts.desc())
        .first())

    if result is None:
        return None

    # strange behavior
    ts_latest = '{:.6f}'.format(float(result.ts) + 1) if result else None

    with m.db.atomic():
        while has_more:
            resp = slack.channels.history(
                latest=ts_latest,
                count=1000,
                channel=channel.id
            ).body

            msglist = resp['messages']

            for msg in msglist:
                # check for difference (status of edition).
                msg_ori = (m.Message.select()
                    .where(m.Message.channel == channel
                        and m.Message.ts == msg['ts'])
                    .first())
                # check if they are exactly the same
                # will msg_ori always exist?
                if msg_ori and msg_ori.edit != msg.get('edited', None):
                    # update, taking original id
                    # FIXME: prevent duplicating objects
                    msg['channel'] = channel
                    process_message(msg)
                    if 'reactions' in msg:
                        insert_reactions(msg['reactions'], 'message', msg['ts'], channel)
                    msg_new = m.Message.api(msg)
                    msg_new.id = msg_ori.id
                    msg_new.save()
                    list_mod.append(msg_new)

            if len(msglist):
                # the list is always sorted by ts desc
                ts_latest = msglist[-1]['ts']

            has_more = resp['has_more']

    return list_mod

def fetch_all_channel_message():
    lst = []
    for chan in m.Channel.select().iterator():
        lst.append(chan)

    cnt_ttl_add = 0
    cnt_ttl_mod = 0
    cnt_ttl = 0

    _tmpl = '| {:22.22} | {:>4} | {:>4} | {:>7} |'
    _hr = '+{0:-<24}+{0:-<6}+{0:-<6}+{0:-<9}+'.format('')

    print(_hr)
    print(_tmpl.format('CHANNEL', '+CNT', '~CNT', 'TOTAL'))
    print(_hr)
    for chan in lst:
        cnt_add = fetch_channel_message(chan)
        cnt_ttl_add += cnt_add
        cnt_ttl += chan.length
        # exprimental
        list_mod = fetch_channel_message_diff(chan)
        cnt_mod = len(list_mod)
        print(_tmpl.format('#' + chan.name, cnt_add, cnt_mod, chan.length))
    print(_hr)
    print(_tmpl.format('--- TOTAL ---', cnt_ttl_add, cnt_mod, cnt_ttl))
    print(_hr)

def fetch_all_star_item():
    lst = []
    for usr in m.User.select():
        lst.append(usr)

    _tmpl = '| {:22.22} | {:>4} | {:>4} |'
    _hr = '+{0:-<24}+{0:-<6}+{0:-<6}+'.format('')

    with m.db.atomic():
        print(_hr)
        print(_tmpl.format('USER', 'PREV', 'CNT'))
        print(_hr)

        cnt_ttl_prev = 0
        cnt_ttl_now = 0

        for usr in lst:
            if usr.is_bot:  # `user_is_bot` error
                continue

            cnt_prev = m.Star.delete().where(m.Star.user == usr).execute()
            cnt_ttl_prev += cnt_prev

            cnt = 0
            page_total = -1
            page = 1
            while page_total < 0 or page < page_total:
                resp = slack.stars.list(
                    user=usr.id,
                    count=1000,
                    page=page
                ).body
                page_total = resp['paging']['pages']
                items = resp['items']
                for item in items:
                    if not m.Star.isPublic(item['type']):
                        continue
                    item['user'] = usr.id
                    m.Star.api(item, True)
                    cnt += 1
                page += 1

            print(_tmpl.format('@' + usr.name, cnt_prev, cnt))
            cnt_ttl_now += cnt

        print(_hr)
        print(_tmpl.format('--- TOTAL ---', cnt_ttl_prev, cnt_ttl_now))
        print(_hr)


def init():
    # modify the name of the database here
    m.db.init('slack-archv.test.sqlite')

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
    print('Fetching Emoji list...')
    fetch_emoji_list()
    print('Fetching all messages from channels...')
    fetch_all_channel_message()
    print('Fetching all starred items from users...')
    fetch_all_star_item()

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

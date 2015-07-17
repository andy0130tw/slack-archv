import json
import datetime
import re

from peewee import *

db = SqliteDatabase('slack-archv-test.sqlite')

def copy_keys(a, b, args):
    for key in args:
        a[key] = b.get(key, None)
    return a

def del_keys(d, args):
    for key in args:
        if key in d:
            del d[key]
    return d

class SlackIDField(CharField):
    '''Field for storing Slack-generated IDs, usually 9 digits.
        Expected to be primary keys'''
    max_length = 9

class JSONField(TextField):
    '''Field for storing stringified-JSON'''
    def db_value(self, value):
        if ((isinstance(value, dict) or isinstance(value, list)) and len(value) == 0
            or value is None):
            return None
        return json.dumps(value, ensure_ascii=False)

    def python_value(self, value):
        if value is None:
            return None
        return json.loads(value)

# class TimestampField(DateTimeField):
#     '''Field for ts; only for setting format'''
#     formats = '%Y-%m-%d %H:%M:%S.%f'

class ModelBase(Model):
    '''Super class for basic models'''
    # transform first upon creation
    @classmethod
    def api(cls, resp, save=False):
        try:
            data = cls._transform(resp)
        except AttributeError:
            data = resp

        if save:
            model, _ = cls.create_or_get(**data)
            return model
        else:
            return cls(**data)

    # transform first before bulk insertion
    @classmethod
    def api_insert_many(cls, rows):
        try:
            trans = cls._transform
            new_rows = [ trans(row) for row in rows ]
        except AttributeError:
            new_rows = rows
        return cls.insert_many(new_rows)

    class Meta:
        database = db

class Information(ModelBase):
    '''As a hash map of team information and metadata'''
    key = CharField(primary_key=True)
    value = CharField(null=True)

class User(ModelBase):
    id = SlackIDField(primary_key=True)
    name = CharField(null=True)
    realname = CharField(null=True)
    name_data = JSONField(null=True)
    is_admin = BooleanField(null=True)
    is_owner = BooleanField(null=True)
    is_bot = BooleanField(null=True)
    avatar = TextField()
    avatar_data = JSONField(null=True)
    timezone = TextField(null=True)
    email = TextField(null=True)
    skype = TextField(null=True)
    phone = TextField(null=True)
    title = TextField(null=True)
    deleted = BooleanField(null=True)
    raw = JSONField(null=True)

    INTACT_KEYS_1 = ['id', 'deleted', 'name', 'is_admin', 'is_owner', 'is_bot']
    INTACT_KEYS_2 = ['email', 'skype', 'phone', 'title']
    INTACT_KEYS_3 = ['first_name', 'last_name', 'real_name_normalized']
    REMOVED_KEYS = INTACT_KEYS_1 + ['profile', 'tz', 'real_name']

    @classmethod
    def getByID(cls, id):
        try:
            return cls.get(cls.id == id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def _transform(cls, resp):
        raw = resp.copy()
        user = {
            'name_data': {},
            'timezone': raw.get('tz', None),
            'realname': raw.get('real_name', None),
            'avatar': raw['profile'].get('image_original', raw['profile'].get('image_192', None)),
            'avatar_data': {key: val for key, val in raw['profile'].items() if key.find('image_') == 0},
            'raw': raw
        }

        copy_keys(user, raw, cls.INTACT_KEYS_1)
        copy_keys(user, raw['profile'], cls.INTACT_KEYS_2)
        copy_keys(user['name_data'], raw['profile'], cls.INTACT_KEYS_3)

        # todo: remove more used keys
        del_keys(raw, cls.REMOVED_KEYS)

        return user

class File(ModelBase):
    id = SlackIDField(primary_key=True)
    # todo: add more fields
    title = TextField()
    mode = CharField()
    filetype = CharField()
    mimetype = TextField()
    permalink = TextField()
    url = TextField()
    url_data = JSONField(null=True)
    thumb_data = JSONField(null=True)
    size = IntegerField()
    is_external = BooleanField()
    preview = TextField(null=True)
    preview_highlight = TextField(null=True)
    created = DateTimeField()
    raw = JSONField(null=True)
    content = BlobField(null=True)

    REX_PERMALINK = re.compile(r'(?:https://[a-z0-9_]+\.slack\.com)?(.+)$')
    REX_URL = re.compile(r'(?:https://slack-files\.com)?(.+)$')
    INTACT_KEYS = [
        'id', 'title', 'mode', 'filetype', 'mimetype', 'size', 'is_external',
        'preview', 'preview_highlight', 'created'
    ]
    # 'permalink_public' can be accessed without permission! unsafe for archive.
    REMOVED_KEYS = INTACT_KEYS + [
        'permalink', 'url', 'permalink_public', 'is_starred',
        'channels', 'ims', 'groups', 'pinned_to', 'initial_comment',
        'num_starred', 'comments_count'
    ]

    @classmethod
    def _transform(cls, resp):
        # unfinished part...
        raw = resp.copy()
        _file = {
            'raw': raw,
            # strip out domain part of links
            'permalink': re.sub(cls.REX_PERMALINK, r'\1', raw.get('permalink', '')),
            'url': re.sub(cls.REX_URL, r'\1', raw.get('url', '')),
            'url_data': {},
            'thumb_data': {}
        }

        copy_keys(_file, raw, cls.INTACT_KEYS)
        del_keys(raw, cls.REMOVED_KEYS)

        # iterate over residue parameters
        del_keys_more = []
        for key, val in raw.items():
            is_thumb_data = (key.find('thumb_') == 0)
            is_url_data   = (key.find('url_')   == 0)
            if is_thumb_data or is_url_data:
                if isinstance(val, str):
                    val = re.sub(cls.REX_URL, r'\1', val)
                if is_thumb_data:
                    _file['thumb_data'][key] = val
                elif is_url_data:
                    _file['url_data'][key] = val
                del_keys_more.append(key)
        del_keys(raw, del_keys_more)

        return _file

class FileComment(ModelBase):
    id = SlackIDField(primary_key = True)
    ts = DateTimeField(null = True)
    file = ForeignKeyField(File, null = True)
    user = ForeignKeyField(User, null = True)
    text = TextField(null = True)

    @classmethod
    def _transform(cls, resp, Fid):
        raw = resp.copy()
        cm = {
            'id': raw.get('id', None),
            'ts': raw.get('timestamp', None),
            'user': raw.get('user', None),
            'text': raw.get('comment', None),
            'file': Fid
        }
        return cm

    @classmethod
    def api_insert_many(cls, rows, fid):
        try:
            trans = cls._transform
            new_rows = [ trans(row, Fid = fid) for row in rows ]
        except AttributeError:
            new_rows = rows
        return cls.insert_many(new_rows)


class Attachment(ModelBase):
    id = PrimaryKeyField()
    title = TextField(null=True)
    text = TextField(null=True)
    link = TextField(null=True)
    from_url = TextField(null=True)
    fallback = TextField(null=True)
    raw = JSONField(null=True)

    @classmethod
    def _transform(cls, resp):
        raw = resp.copy()
        attachment = {
          'link': resp.get('title_link', None),
          'raw': raw
        }
        intact_keys = ['title', 'fallback', 'text', 'from_url']
        copy_keys(attachment, raw, intact_keys)
        # id always equal "1", not knowing its purpose
        del_keys(raw, intact_keys + ['title_link', 'id'])
        return attachment

class DirectMessage(ModelBase):
    id = CharField(primary_key=True)
    user = ForeignKeyField(User, unique=True)
    # created = DateTimeField()
    user_deleted = BooleanField(null=True)
    class Meta:
        db_table = 'directMessage'

class ModelSlackMessageList(ModelBase):
    '''as a super class of channels and groups'''
    name = CharField()
    created = DateTimeField()
    creator = ForeignKeyField(User)
    archived = BooleanField(null=True)
    topic = JSONField()
    purpose = JSONField()
    # latest = DateTimeField()

    @property
    def members(self):
        return ChannelUser.select().where(ChannelUser.channel.id == self.id)

    @classmethod
    def getByID(cls, id):
        try:
            return cls.get(cls.id == id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def getByName(cls, name):
        try:
            return cls.get(cls.name == name)
        except cls.DoesNotExist:
            return None


    @classmethod
    def _transform(cls, resp):
        msglist = {
            'archived': resp['is_archived']
        }
        return copy_keys(msglist, resp, ['id', 'name', 'created', 'creator', 'topic', 'purpose'])

class Channel(ModelSlackMessageList):
    # looks like peewee can't inherit primary keys from super classes
    id = SlackIDField(primary_key=True)

class Group(ModelSlackMessageList):
    id = SlackIDField(primary_key=True)


class Message(ModelBase):
    channel = ForeignKeyField(Channel)
    # if null, message is the real message of a user
    #  otherwise it should be only a hint describing raw
    subtype = CharField(null=True)
    text = TextField(null=True)
    ts = DateTimeField(index=True)
    user = ForeignKeyField(User, null=True)
    file = ForeignKeyField(File, null=True)
    attachment = ForeignKeyField(Attachment, null=True)
    edit = JSONField(null=True)
    raw = JSONField(null=True)
    updated = DateTimeField(default=datetime.datetime.now)

    @classmethod
    def _transform(cls, resp):
        raw = resp.copy()
        message = {
            'edit': raw.get('edited', None),
            'raw': raw,
            'attachment': raw.get('_attachment', None),
            'file': raw.get('_file', None)
        }
        # todos:
        #  fetching user is tricky when file is present
        #  bot user breaking foreign key
        intact_keys = ['channel', 'subtype', 'text', 'ts', 'user']
        copy_keys(message, raw, intact_keys)

        subtype = raw.get('subtype', '')
        # *: do some small modifications to make the text more representative?
        # if subtype == 'file_share':
            # *: initial comment?
            # message['text'] = raw['file']['initial_comment']['comment']
            # del raw['file']['initial_comment']['comment']
        if subtype == 'file_comment':
            message['user'] = raw['comment']['user']
            # *: real comment?
            # message['text'] = raw['comment']['comment']
            # del raw['comment']

        # `type` field is always `'message'` if present
        # `is_starred` field is private, do not insert it
        del_keys(raw, intact_keys + ['type', 'edited', '_attachment', '_file', 'is_starred'])

        return message

class ChannelUser(ModelBase):
    channel = ForeignKeyField(Channel)
    user = ForeignKeyField(User)
    class Meta:
        db_table = 'channelUser'

class ModelSlackStarList(ModelBase):
    '''as a super class of user starred items,
        both public and private ones'''
    user = ForeignKeyField(User)
    # channel, message, file, file_comment
    # for StarPrivate only: im, group
    item_type = CharField()
    item_id = CharField()

class Star(ModelSlackStarList):
    pass

class StarPrivate(ModelSlackStarList):
    # either im or group
    item_source = CharField()
    class Meta:
        db_table = 'starPrivate'

# Experimental feature on Slack
class Reaction(ModelBase):
    message = ForeignKeyField(Message)
    reaction = CharField()
    user = ForeignKeyField(User)


def init_models():
    '''Create tables by model definitions.'''
    with db.atomic():
        db.create_tables([
            Information,
            User,
            Message,
            File,
            Attachment,
            DirectMessage,
            Channel,
            Group,
            ChannelUser,
            Star,
            StarPrivate,
            FileComment
        ], safe=True)

def table_clean():
    '''Remove all temporary data to allow full update.'''
    with db.atomic():
        for model in [User, Channel, Group, ChannelUser, DirectMessage]:
            model.delete()

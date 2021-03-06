import json
import datetime
import re

from peewee import *
from playhouse.shortcuts import model_to_dict
db = SqliteDatabase(None)

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
    ''' Field for storing Slack-generated IDs, usually 9 digits.
        Expected to be primary keys '''
    max_length = 9

class JSONField(TextField):
    ''' Field for storing stringified-JSON '''
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
    ''' Super class for basic models '''
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

    @classmethod
    def api_bulk_insert(cls, rows):
        ''' A dirty workaround.
            Recommended way to do bulk insert with respect to field count,
            avoiding  `peewee.OperationalError: too many SQL variables`.
            Note: No `.execute()` is needed. '''
        # hack into meta data
        # alt. way to do this is `m.User._meta.columns`
        insert_limit = 999 // len(cls._meta.fields)
        for idx in range(0, len(rows), insert_limit):
            cls.api_insert_many(rows[idx:idx+insert_limit]).execute()

    @classmethod
    def getBy(cls, field_name, value):
        ''' Get a single instance by a value of a field. '''
        return cls.select().where(getattr(cls, field_name) == value).first()

    REX_PERMALINK = re.compile(r'(?:https://[a-z0-9_]+\.slack\.com)?(.+)$')
    @classmethod
    def remove_permalink_domain(cls, url):
        return cls.REX_PERMALINK.sub(r'\1', url)

    def _dict(self, delete_empty=True, **kwargs):
        # Experimental
        kwargs['recurse'] = kwargs.get('recurse', False)
        d = model_to_dict(self, **kwargs)
        if delete_empty:
            d = { k: v for k, v in d.items() if v is not None }
        return d

    class Meta:
        database = db

class Information(ModelBase):
    ''' As a hash map of team information and metadata '''
    key = CharField(primary_key=True)
    value = CharField(null=True)

class User(ModelBase):
    id = SlackIDField(primary_key=True)
    name = CharField(null=True, unique=True)
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

FileCommentProxy = Proxy()

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
    initial_comment = ForeignKeyField(FileCommentProxy, null=True)
    raw = JSONField(null=True)
    content = BlobField(null=True)

    REX_URL = re.compile(r'(?:https://slack-files\.com)?(.+)$')

    INTACT_KEYS = [
        'id', 'title', 'mode', 'filetype', 'mimetype', 'size', 'is_external',
        'preview', 'preview_highlight', 'created'
    ]
    # 'permalink_public' can be accessed without permission! unsafe for archive.
    # 'timestamp' is mentioned in Slack API as follows:
    #  > The timestamp property contains the same data as created,
    #    but is deprecated and is provided only for backwards compatibility
    #    with older clients.
    REMOVED_KEYS = INTACT_KEYS + [
        'permalink', 'url', 'permalink_public', 'is_starred',
        'channels', 'ims', 'groups', 'pinned_to',
        'num_starred', 'comments_count', 'initial_comment', 'timestamp'
    ]

    @classmethod
    def _transform(cls, resp):
        # unfinished part...
        raw = resp.copy()
        _file = {
            'raw': raw,
            # strip out domain part of links
            'permalink': cls.remove_permalink_domain(raw.get('permalink', '')),
            'url': cls.REX_URL.sub(r'\1', raw.get('url', '')),
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
    id = SlackIDField(primary_key=True)
    file = ForeignKeyField(File)
    created = DateTimeField(null=True)
    user = ForeignKeyField(User, null=True)
    comment = TextField(null=True)

    INTACT_KEYS = ['id', 'created', 'user', 'comment']

    @classmethod
    def _transform(cls, resp):
        # transforming from comment in message
        comment = {
            'file': resp.get('_file', None)
        }
        copy_keys(comment, resp, cls.INTACT_KEYS)
        return comment

    class Meta:
        db_table = 'fileComment'

FileCommentProxy.initialize(FileComment)

class Attachment(ModelBase):
    id = PrimaryKeyField()
    title = TextField(null=True)
    text = TextField(null=True)
    link = TextField(null=True)
    from_url = TextField(null=True)
    fallback = TextField(null=True)
    raw = JSONField(null=True)

    INTACT_KEYS = ['title', 'fallback', 'text', 'from_url']
    REMOVED_KEYS = ['title_link', 'id']

    @classmethod
    def _transform(cls, resp):
        raw = resp.copy()
        attachment = {
          'link': resp.get('title_link', None),
          'raw': raw
        }
        copy_keys(attachment, raw, cls.INTACT_KEYS)
        # id always equals to "1", not knowing its purpose
        del_keys(raw, cls.INTACT_KEYS + cls.REMOVED_KEYS)
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
    name = CharField(unique=True)
    created = DateTimeField()
    creator = ForeignKeyField(User)
    archived = BooleanField(null=True)
    topic = JSONField()
    purpose = JSONField()
    # latest = DateTimeField()

    @property
    def members(self):
        return self.__class__.select().join(ChannelUser).where(ChannelUser.channel == self)

    @classmethod
    def _transform(cls, resp):
        msglist = {
            'archived': resp['is_archived']
        }
        return copy_keys(msglist, resp, ['id', 'name', 'created', 'creator', 'topic', 'purpose'])

class Channel(ModelSlackMessageList):
    # looks like peewee can't inherit primary keys from super classes
    id = SlackIDField(primary_key=True)

    INTACT_KEYS = ['id', 'name', 'created', 'creator', 'topic', 'purpose']

    @property
    def length(self):
        return Message.select().where(Message.channel == self).count()

    @classmethod
    def _transform(cls, resp):
        msglist = {
            'archived': resp['is_archived']
        }
        return copy_keys(msglist, resp, cls.INTACT_KEYS)

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

    INTACT_KEYS = ['channel', 'subtype', 'text', 'ts', 'user']
    REMOVED_KEYS = INTACT_KEYS + [
        'type', 'edited', '_attachment', '_file', 'is_starred',
        'comment'
    ]

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
        copy_keys(message, raw, cls.INTACT_KEYS)

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
        del_keys(raw, cls.REMOVED_KEYS)

        return message

    def _dict(self, merge_raw=False, **kwargs):
        message = super()._dict(**kwargs)
        del message['id']
        del message['updated']
        if 'raw' in message:
            if merge_raw:
                message.update(message['raw'])
            del message['raw']

        # todo: file
        # todo: comment
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
    permalink = TextField(null=True)

    @staticmethod
    def isPublic(item_type):
        return item_type in ['channel', 'message', 'file', 'file_comment'];

class Star(ModelSlackStarList):
    @classmethod
    def _transform(cls, resp):
        _type = resp['type']
        star = {
            'user': resp['user'],
            'item_type': _type
        }
        if _type == 'channel':
            star['item_id'] = resp['channel']
        elif _type == 'message':
            # somehow strange; use the format of permalink
            # or search the exact item in DB?
            star['item_id'] = resp['channel'] + '/' + resp['message']['ts']
            star['permalink'] = cls.remove_permalink_domain(resp['message']['permalink'])
        elif _type == 'file':
            star['item_id'] = resp['file']['id']
            star['permalink'] = cls.remove_permalink_domain(resp['file']['permalink'])
        elif _type == 'file_comment':
            # including file id?
            star['item_id'] = resp['comment']['id']
        else:
            # not recognized (or private) star
            raise BaseException('Failed when creating Star model. Not recognized or private star list item.')
        return star

class StarPrivate(ModelSlackStarList):
    # either im or group
    item_source = CharField()

    class Meta:
        db_table = 'starPrivate'

# Experimental feature on Slack
class Reaction(ModelBase):
    item_type = CharField(null=True)
    item_id = DateTimeField(index=True)
    channel = ForeignKeyField(Channel, null=True)
    reaction = CharField()
    user = ForeignKeyField(User)

class Emoji(ModelBase):
    emoji = CharField(unique=True)
    url = TextField()

    @classmethod
    def _transform(cls, resp):
        # a tuple is expected as response
        return {
            'emoji': resp[0],
            'url': cls.remove_permalink_domain(resp[1])
        }

def init_models():
    ''' Create tables by model definitions. '''
    with db.atomic():
        db.create_tables([
            Information,
            User,
            Message,
            File,
            Attachment,
            # DirectMessage,
            Channel,
            # Group,
            ChannelUser,
            Star,
            # StarPrivate,
            FileComment,
            Reaction,
            Emoji
        ], safe=True)

def table_clean():
    ''' Remove all temporary data to allow full update. '''
    with db.atomic():
        for model in [User, Channel, ChannelUser, Emoji]:
            model.delete().execute()

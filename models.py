import json
import datetime

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
        return json.loads(value)

# class TimestampField(DateTimeField):
#     '''Field for ts; only for setting format'''
#     formats = '%Y-%m-%d %H:%M:%S.%f'

class ModelBase(Model):
    '''Super class for basic models'''
    # transform first upon creation
    @classmethod
    def api(cls, resp):
        try:
            data = cls._transform(resp)
        except AttributeError:
            data = resp
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
            'avatar_data': {key: val for key, val in raw['profile'].items() if key.find('image_') == 0}
        }

        copy_keys(user, raw, ['id', 'deleted', 'name', 'is_admin', 'is_owner', 'is_bot'])
        copy_keys(user, raw['profile'], ['email', 'skype', 'phone', 'title'])
        copy_keys(user['name_data'], raw['profile'], ['first_name', 'last_name', 'real_name_normalized'])

        # todo: remove more used keys
        del raw['profile']
        user['raw'] = raw

        return user

class File(ModelBase):
    id = SlackIDField(primary_key=True)
    # todo: add more fields
    permalink = TextField()
    raw = JSONField(null=True)
    content = BlobField(null=True)

    @classmethod
    def _transform(cls, resp):
        # unfinished part...
        raw = resp.copy()
        _file = {
            'raw': raw
        }
        intact_keys = ['id', 'permalink']
        copy_keys(_file, raw, intact_keys)
        del_keys(raw, intact_keys)
        return _file

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
    ts = DateTimeField()
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
        #  file/attachment detection
        #  edit detection
        intact_keys = ['channel', 'subtype', 'text', 'ts', 'user']
        copy_keys(message, raw, intact_keys)
        # this field is always 'message' if present
        del_keys(raw, intact_keys + ['type', 'edited', '_attachment', '_file'])

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
            StarPrivate
        ], safe=True)

def table_clean():
    '''Remove all temporary data to allow full update.'''
    with db.atomic():
        for model in [User, Channel, Group, ChannelUser, DirectMessage]:
            model.delete()

import json
import datetime

from peewee import *

db = SqliteDatabase('slack-archv-test.sqlite')

class SlackIDField(CharField):
    '''Field for storing Slack-generated IDs, usually 9 digits.
        Expected to be primary keys'''
    max_length = 9

class JSONField(TextField):
    '''Field for storing stringified-JSON'''
    def db_value(self, value):
        if (isinstance(value, dict) or isinstance(value, list)) and len(value) == 0:
            return None
        return json.dumps(value, ensure_ascii=False)

    def python_value(self, value):
        return json.loads(value)

# class TimestampField(DateTimeField):
#     '''Field for ts; only for setting format'''
#     formats = '%Y-%m-%d %H:%M:%S.%f'

class ModelBase(Model):
    '''Super class for basic models'''
    class Meta:
        database = db

class Information(ModelBase):
    '''As a hash map of team information and metadata'''
    key = CharField(primary_key=True)
    value = CharField(null=True)

class User(ModelBase):
    id = SlackIDField(index=True, primary_key=True)
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
    def api(Class, resp):
        '''Transform data from API response.'''
        raw = resp.copy()
        user = {
            'name_data': {},
            'timezone': raw.get('tz', None),
            'realname': raw.get('real_name', None),
            'avatar': raw['profile'].get('image_original', raw['profile'].get('image_192', None)),
            'avatar_data': {key: val for key, val in raw['profile'].items() if key.find('image_') == 0}
        }
        for key in ['id', 'deleted', 'name', 'is_admin', 'is_owner', 'is_bot']:
            user[key] = raw.get(key, None)
        for key in ['email', 'skype', 'phone', 'title']:
            user[key] = raw['profile'].get(key, None)
        for key in ['first_name', 'last_name', 'real_name_normalized']:
            user['name_data'][key] = raw['profile'].get(key, None)

        #todo: remove more used keys
        del raw['profile']
        user['raw'] = raw

        return Class.create(**user)

class File(ModelBase):
    id = SlackIDField(index=True, primary_key=True)

class Attachment(ModelBase):
    id = PrimaryKeyField()
    content = JSONField()

class DirectMessage(ModelBase):
    id = CharField(primary_key=True)
    user = ForeignKeyField(User, unique=True)
    # created = DateTimeField()
    user_deleted = BooleanField(null=True)
    class Meta:
        db_table = 'directMessage'

class ModelSlackMessageList(ModelBase):
    '''as a super class of channels and groups'''

    id = SlackIDField(index=True, unique=True, primary_key=True)
    name = CharField()
    created = DateTimeField()
    creator = ForeignKeyField(User)
    archived = BooleanField(null=True)
    topic = JSONField()
    purpose = JSONField()

    def update_with_raw(self, raw):
        self.id = raw['id']
        self.name = raw['name']
        self.created = raw['created']
        try:
            self.creator = User.get(User.id == raw['creator'])
        except User.DoesNotExist:
            pass

        self.archived = raw['is_archived']
        self.topic = raw['topic']
        self.purpose = raw['purpose']
        self.save()

class Channel(ModelSlackMessageList):
    pass

class Group(ModelSlackMessageList):
    pass


class Message(ModelBase):
    id = SlackIDField(index=True, primary_key=True)
    channel = ForeignKeyField(Channel)
    # if null, message is the real message of a user
    #  otherwise it should be only a hint describing raw
    subtype = CharField(null=True)
    message = TextField(null=True)
    created = DateTimeField()
    file = ForeignKeyField(File, null=True)
    attachment = ForeignKeyField(Attachment, null=True)
    edit = JSONField(null=True)
    raw = JSONField(null=True)
    updated = DateTimeField(default=datetime.datetime.now)

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

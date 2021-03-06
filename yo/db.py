# coding=utf-8
import datetime
import json
import logging
import uuid
from contextlib import contextmanager
from enum import IntFlag
from sqlite3 import IntegrityError as SQLiteIntegrityError

import dateutil
import dateutil.parser
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

metadata = sa.MetaData()

NOTIFY_TYPES = ('power_down', 'power_up', 'resteem', 'feed', 'reward', 'send',
                'mention', 'follow', 'vote', 'comment_reply', 'post_reply',
                'account_update', 'message', 'receive')

TRANSPORT_TYPES = ('email', 'sms', 'wwwpoll')


class Priority(IntFlag):
    MARKETING = 1
    LOW = 2
    NORMAL = 3
    PRIORITY = 4
    ALWAYS = 5


DEFAULT_USER_TRANSPORT_SETTINGS = {
    "email": {
        "notification_types": [],
        "sub_data": {}
    },
    "wwwpoll": {
        "notification_types": [
            "power_down", "power_up", "resteem", "feed", "reward", "send",
            "mention", "follow", "vote", "comment_reply", "post_reply",
            "account_update", "message", "receive"
        ],
        "sub_data": {}
    }
}

DEFAULT_USER_TRANSPORT_SETTINGS_STRING = json.dumps(
    DEFAULT_USER_TRANSPORT_SETTINGS)

# This is the table queried by API server for the wwwpoll transport
wwwpoll_table = sa.Table(
    'yo_wwwpoll',
    metadata,
    sa.Column('nid', sa.String(36), primary_key=True),
    sa.Column('notify_type', sa.String(20), nullable=False, index=True),
    sa.Column('to_username', sa.String(20), nullable=False, index=True),
    sa.Column(
        'from_username', sa.String(20), nullable=False, index=True
    ),  # TODO - do we actually need this? @jg please discuss as you keep adding references to this field
    sa.Column('json_data', sa.UnicodeText(1024)),

    # wwwpoll specific columns
    sa.Column(
        'created',
        sa.DateTime,
        default=sa.func.now(),
        nullable=False,
        index=True),
    sa.Column(
        'updated',
        sa.DateTime,
        default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
        index=True),
    sa.Column('read', sa.Boolean(), default=False),
    sa.Column('shown', sa.Boolean(), default=False),

    #    sa.UniqueConstraint('to_username','notify_type','json_data',name='yo_wwwpoll_idx'),
    mysql_engine='InnoDB',
)

# This is where ALL notifications go, not to be confused with the wwwpoll
# transport specific table above
notifications_table = sa.Table(
    'yo_notifications',
    metadata,
    sa.Column('nid', sa.String(36), primary_key=True),
    sa.Column('notify_type', sa.String(20), nullable=False, index=True),
    sa.Column('to_username', sa.String(20), nullable=False, index=True),
    sa.Column('from_username', sa.String(20), index=True, nullable=True),
    sa.Column('json_data', sa.UnicodeText(1024)),
    sa.Column(
        'created',
        sa.DateTime,
        default=sa.func.now(),
        nullable=False,
        index=True),
    sa.Column(
        'updated',
        sa.DateTime,
        default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
        index=True),

    # non-wwwpoll columns
    sa.Column('priority_level', sa.Integer, index=True, default=3),
    sa.Column('created_at', sa.DateTime, default=sa.func.now(), index=True),
    sa.Column('trx_id', sa.String(40), index=True, nullable=True),
    sa.UniqueConstraint(
        'to_username',
        'notify_type',
        'trx_id',
        'from_username',
        name='yo_notification_idx'),
    mysql_engine='InnoDB',
)

actions_table = sa.Table(
    'yo_actions',
    metadata,
    sa.Column('aid', sa.Integer, primary_key=True),
    sa.Column('nid', None, sa.ForeignKey('yo_notifications.nid')),
    sa.Column('to_username', sa.String(20), nullable=False, index=True), # yes, denormalised - this is so we can archive the 2 tables seperately
    sa.Column('transport', sa.String(20), nullable=True, index=True),
    sa.Column('priority_level', sa.Integer, index=True, default=3),
    sa.Column('status', sa.String(20), nullable=False, index=True),
    sa.Column('created_at', sa.DateTime, default=sa.func.now(), index=True),
    sa.UniqueConstraint('aid', 'nid', 'transport', name='yo_wwwpoll_idx'),
    mysql_engine='InnoDB',
)

user_settings_table = sa.Table(
    'yo_user_settings',
    metadata,
    sa.Column('username', sa.String(20), primary_key=True),
    sa.Column(
        'transports',
        sa.UnicodeText,
        index=False,
        default=DEFAULT_USER_TRANSPORT_SETTINGS_STRING,
        nullable=False),
    sa.Column('created', sa.DateTime, default=sa.func.now(), index=False),
    sa.Column(
        'updated',
        sa.DateTime,
        default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
        index=True),
    mysql_engine='InnoDB',
)

# the below table should have only a single row at any one time
chain_status_table = sa.Table(
    'yo_chain_status',
    metadata,
    sa.Column('status_id', sa.Integer, primary_key=True),
    sa.Column('last_processed_block', sa.Integer, index=True),
    sa.Column('last_processed_time', sa.DateTime, index=True),
    sa.Column('lock_expires', sa.DateTime, index=True),   # set to the future to keep locked, set to the past to release lock
    sa.Column('active_follower_id',   sa.String(36), index=True),
    mysql_engine='InnoDB',
)


def is_duplicate_entry_error(error):
    if isinstance(error, (IntegrityError, SQLiteIntegrityError)):
        msg = str(error).lower()
        return "unique" in msg
    return False


# pylint: disable-msg=no-value-for-parameter
class YoDatabase:
    def __init__(self, db_url=None):
        self.db_url = db_url
        self.engine = sa.create_engine(self.db_url)
        self.metadata = metadata
        self.metadata.create_all(bind=self.engine)
        self.url = make_url(self.db_url)

    @contextmanager
    def acquire_conn(self):
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    @property
    def backend(self):
        return self.url.get_backend_name()
    
    def get_chain_status(self):
        """ Returns current blockchain status
        """
        retval = None
        with self.acquire_conn() as conn:
             query = chain_status_table.select()
             resp = conn.execute(query)
             if resp is not None:
                resp = resp.fetchone()
                if resp is not None:
                   retval = dict(resp.items())
        return retval

    def try_update_status(self,follower_id=None, last_processed_block=None, lock_timeout=5):
        """ Tries to update the chain status

        Fails if one of the following occurs:
            last_processed_block param <= current last_processed_block in DB
            active_follower_id != follower_id

        Otherwise, last_processed_block is updated, last_processed_time is set to now and lock_expires is set to the current time + lock_timeout

        Returns the new chain_status, which may or may not be updated
        """
        now = datetime.datetime.now()
        with self.acquire_conn() as conn:
             tx = conn.begin()
             query = chain_status_table.update(values=dict(last_processed_block= last_processed_block,
                                                           last_processed_time = now,
                                                           lock_expires        = now + datetime.timedelta(seconds = lock_timeout)))
             query = query.where(chain_status_table.c.active_follower_id == follower_id)
             query = query.where(chain_status_table.c.last_processed_block <= last_processed_block)
             try:
                conn.execute(query)
                tx.commit()
             except:
                tx.rollback()

        return self.get_chain_status()


    def try_active_follower(self,follower_id=None,last_processed_block=None,lock_timeout=5):
        """ Tries to set the currently active blockchain follower to the ID provided

        This only succeeds if the current active follower's lock has expired and there's not a currently open transaction etc

        Not to be confused with try_update_status() above

        last_processed_block will only be changed if the DB is empty, in which case the timestamp will be set to the current time

        lock_timeout is the expiry time in seconds of the lock

        Returns the new blockchain status, which may or may not be updated
        """
        now =  datetime.datetime.now()
        with self.acquire_conn() as conn:
             tx = conn.begin()
             # first get the current status
             query = chain_status_table.select()
             resp  = conn.execute(query).fetchone()
             if resp is None:
                query = chain_status_table.insert(values=dict(active_follower_id=follower_id,
                                                              last_processed_block=last_processed_block,
                                                              last_processed_time = now,
                                                              lock_expires        = now + datetime.timedelta(seconds = lock_timeout)))
             
             else:
                query = chain_status_table.update(values=dict(active_follower_id=follower_id,
                                                              lock_expires=now + datetime.timedelta(seconds = lock_timeout)))

                query = query.where(chain_status_table.c.lock_expires <= now)
             try:
                resp = conn.execute(query)
                tx.commit()
             except:
                tx.rollback()
        return self.get_chain_status()


    def _get_notifications(self,
                           table=None,
                           nid=None,
                           to_username=None,
                           created_before=None,
                           updated_after=None,
                           read=None,
                           notify_types=None,
                           limit=30):
        """Returns an SQLAlchemy result proxy with the notifications stored in wwwpoll table matching the specified params

       Keyword args:
          nid(int):            notification id
          username(str):       the username to lookup notifications for
          created_before(str): ISO8601-formatted timestamp
          updated_after(str):  ISO8601-formatted
          read(bool):          if set, only return notifications where the read flag is set to this value
          notify_types(list):  if set, only return notifications of one of the types specified in this list
          limit(int):          return at most this number of notifications

       Returns:
          list
       """
        with self.acquire_conn() as conn:
            try:
                query = table.select()
                if nid:
                    return conn.execute(query.where(table.c.nid == nid))
                if to_username:
                    query = query.where(table.c.to_username == to_username)
                if created_before:
                    created_before_val = dateutil.parser.parse(created_before)
                    query = query.where(table.c.created >= created_before_val)
                if updated_after:
                    updated_after_val = dateutil.parser.parse(updated_after)
                    query = query.where(table.c.updated <= updated_after_val)
                if read:
                    query = query.where(table.c.read == read)
                if notify_types:
                    query = query.filter(table.c.notify_type.in_(notify_types))
                query = query.limit(limit)
                resp = conn.execute(query)
                if resp is not None:
                    return resp.fetchall()
            except BaseException:
                logger.exception('_get_notifications failed')
        return []

    def get_notifications(self, **kwargs):
        kwargs['table'] = notifications_table
        return self._get_notifications(**kwargs)

    def get_wwwpoll_notifications(self, **kwargs):
        kwargs['table'] = wwwpoll_table
        return self._get_notifications(**kwargs)

    def get_wwwpoll_unsents(self):
        retval = {}
        with self.acquire_conn() as conn:
            query = sa.sql.select([notifications_table.c.nid])
            query = query.except_(
                sa.sql.select([
                    actions_table.c.nid
                ]).where(actions_table.c.nid == notifications_table.c.nid))
            select_response = conn.execute(query).fetchall()
            logger.info(str(select_response))

            for nid in select_response:
                row = conn.execute(
                    sa.sql.select([
                        notifications_table
                    ]).where(notifications_table.c.nid == nid[0])).fetchone()
                if not row['to_username'] in retval.keys():
                    retval[row['to_username']] = []
                retval[row['to_username']].append(dict(row.items()))
        return retval

    def _create_notification(self, conn=None, table=None, **notification):
            tx = conn.begin()
            try:
                result = conn.execute(table.insert(), **notification)
                logger.debug('_create_notification response: %s', result)

                tx.commit()
                return True
            except (IntegrityError, SQLiteIntegrityError) as e:
                if is_duplicate_entry_error(e):
                    logger.debug(
                        '_create_notification ignoring duplicate entry error')
                    return True
                else:
                    logger.exception(
                        '_create_notification failed to add notification')
                    tx.rollback()
            except BaseException:
                tx.rollback()
                logger.exception('_create_notification failed for %s',
                                 notification)
            return False

    def mark_sent(self, notification_object, transport):
        """ Marks a notification as sent (updates sent_at timestamp)
        """
        now = datetime.datetime.now()
        logger.debug('DB: Marking %s as sent via transport \'%s\' at %s', str(notification_object), transport, str(now))
        with self.acquire_conn() as conn:
             tx = conn.begin()
             try:
                query = actions_table.insert(values=dict(nid=notification_object['nid'],
                                                         transport=transport,
                                                         to_username=notification_object['to_username'],
                                                         status='Sent',
                                                         created_at=now,
                                                         priority_level=notification_object['priority_level']))
                conn.execute(query)
                tx.commit()
             except:
                logger.exception('Exception occurred while marking %s as sent', nid)
                tx.rollback()

    def wwwpoll_mark_shown(self, nid):
        logger.debug('wwwpoll: marking %s as shown', nid)
        with self.acquire_conn() as conn:
            try:
                query = wwwpoll_table.update() \
                    .where(wwwpoll_table.c.nid == nid) \
                    .values(shown=True)
                conn.execute(query)
                return True
            except BaseException:
                logger.exception('wwwpoll_mark_shown failed')
        return False

    def wwwpoll_mark_unshown(self, nid):
        logger.debug('wwwpoll: marking %s as unshown', nid)
        with self.acquire_conn() as conn:
            try:
                query = wwwpoll_table.update() \
                    .where(wwwpoll_table.c.nid == nid) \
                    .values(shown=False)
                conn.execute(query)
                return True
            except BaseException:
                logger.exception('wwwpoll_mark_unshown failed')
        return False

    def wwwpoll_mark_read(self, nid):
        logger.debug('wwwpoll: marking %s as read', nid)
        with self.acquire_conn() as conn:
            try:
                query = wwwpoll_table.update() \
                    .where(wwwpoll_table.c.nid == nid) \
                    .values(read=True)
                conn.execute(query)
                return True
            except BaseException:
                logger.exception('wwwpoll_mark_read failed')
        return False

    def wwwpoll_mark_unread(self, nid):
        logger.debug('wwwpoll: marking %s as unread', nid)
        with self.acquire_conn() as conn:
            try:
                query = wwwpoll_table.update() \
                    .where(wwwpoll_table.c.nid == nid) \
                    .values(read=False)
                conn.execute(query)
                return True
            except BaseException:
                logger.exception('wwwpoll_mark_unread failed')
        return False

    def create_user(self, username, transports=None):
        logger.info('Creating user %s', username)
        if transports is None:
            transports = DEFAULT_USER_TRANSPORT_SETTINGS
        user_settings_data = {
            'username': username,
            'transports': json.dumps(transports)
        }
        success = False
        with self.acquire_conn() as conn:
            try:
                stmt = user_settings_table.insert(values=user_settings_data)
                _ = conn.execute(stmt)
                if _ is not None:
                    logger.info('Created user %s with settings %s', username,
                                json.dumps(transports))
                    success = True
            except BaseException:
                logger.exception('create_user failed')
                success = False
        return success

    def get_user_transports(self, username=None, retry=False):
        """Returns the JSON object representing user's configured transports

       This method does no validation on the object, it is assumed that the object was validated in set_user_transports

       Args:
          username(str): the username to lookup

       Returns:
          dict: the transports configured for the user
       """
        retval = None
        with self.acquire_conn() as conn:
            try:
                query = user_settings_table.select().where(
                    user_settings_table.c.username == username)
                select_response = conn.execute(query)
                results = select_response.fetchone()
                if results is not None:
                    json_settings = results['transports']
                    retval = json.loads(json_settings)
                else:
                    retval = None
            except BaseException:
                logger.exception('get_user_transports failed')
        if (retval is None) and (not retry):
            if self.create_user(username):
                return self.get_user_transports(username=username, retry=True)
            else:
                logger.error('get_user_transports failed')
        return retval

    def set_user_transports(self, username=None, transports=None):
        """ Sets the JSON object representing user's configured transports
        This method does only basic sanity checks, it should only be invoked via the API server
        Args:
            username(str):    the user whose transports need to be set
            transports(dict): maps transports to dicts containing 'notification_types' and 'sub_data' keys
        """
        with self.acquire_conn() as conn:
            # user exists
            # user doesnt exist
            success = False
            try:
                stmt = user_settings_table.update().where(
                    user_settings_table.c.username == username). \
                    values(transports=json.dumps(transports))
                result = conn.execute(stmt).fetchone()
                success = True
            except sa.exc.SQLAlchemyError as e:
                logger.info(
                       'Exception occurred trying to update transports for user %s to %s: %s',
                    username, str(transports),str(e))
        if not success:
            result = self.create_user(username, transports=transports)
            if result:
                success = True
        return success

    def get_priority_count(self,
                           to_username,
                           priority,
                           timeframe,
                           start_time=None):
        """Returns count of notifications to a user of a set priority or higher

       This is used to implement the rate limits

       Args:
           to_username(str): The username to lookup
           priority(int):    The priority level to lookup
           timeframe(int):   The timeframe in seconds to check

       Keyword args:
           start_time(datetime.datetime): the current time to go backwards from, if not set datetime.now() will be used

       Returns:
           An integer count of the number of notifications sent to the specified user within the specified timeframe of that priority level or higher
           :param timeframe:
           :param priority:
           :param to_username:
           :param start_time:
       """
        if start_time is None:
            start_time = datetime.datetime.now() - datetime.timedelta(
                seconds=timeframe)
        retval = 0
        with self.acquire_conn() as conn:
            try:
                query = actions_table.select().where(
                    actions_table.c.to_username == to_username)
                query = query.where(
                    actions_table.c.priority_level >= int(priority))
                query = query.where(
                    actions_table.c.created_at >= start_time)
                select_response = conn.execute(query).fetchall()
                if select_response is None: retval = 0
                logger.debug('Existing notifications at priority %d for user %s: %s', int(priority), to_username, str(select_response))
                retval = len(select_response)
            except BaseException:
                logger.exception('Exception occurred!')
        if retval < 0: return 0
        return retval

    def create_wwwpoll_notification(self,
                                    notify_id=None,
                                    notify_type=None,
                                    created_time=None,
                                    json_data=None,
                                    from_username=None,
                                    to_username=None,
                                    shown=False,
                                    read=False):
        """ Creates a new notification in the wwwpoll table

        Keyword Args:
           notify_id(str):    if not provided, will be autogenerated as a UUID
           notify_type(str):  the notification type
           created_time(str): ISO8601-formatted timestamp, if not set current time will be used
           json_data(str):    what to include in the data field of the stored notification, must be JSON formatted
           to_user(str):      the username we're sending to
           shown(bool):       whether or not the notification should start marked as shown (default False)
           read(bool):       whether or not the notification should start marked as shown (default False)

        Returns:
           dict: the notification as stored in wwwpoll, None on error
        """

        if notify_id is None:
            notify_id = str(uuid.uuid4)
        if created_time is None:
            created_time = datetime.datetime.now()
        notification = {
            'nid': notify_id,
            'notify_type': notify_type,
            'created': created_time,
            'updated': created_time,
            'from_username':
            from_username,  # TODO - does this field even make sense for the wwwpoll table?
            'to_username': to_username,
            'shown': shown,
            'json_data': json_data,
            'read': read
        }
        success = False
        with self.acquire_conn() as conn:
            tx = conn.begin()
            try:
                conn.execute(wwwpoll_table.insert(), **notification)
                tx.commit()
                success = True
            except (IntegrityError, SQLiteIntegrityError) as e:
                if is_duplicate_entry_error(e):
                    logger.debug('Ignoring duplicate entry error')
                    success = True
                else:
                    logger.exception('failed to add notification')
                    tx.rollback()
            except BaseException:
                tx.rollback()
                logger.exception(
                    'Failed to create new wwwpoll notification object: %s',
                    notification)
        return success

    def create_notification(self, **notification_object):
        """ Creates an unsent notification in the DB

        Keyword Args:
           notification_object(dict): the actual notification to create+store

        Returns:
          True on success, False on error
        """
        if 'nid' not in notification_object.keys():
            notification_object['nid'] = str(uuid.uuid4())
        with self.acquire_conn() as conn:
            tx = conn.begin()
            try:
                _ = conn.execute(notifications_table.insert(),
                                 **notification_object)
                tx.commit()
                logger.info('Created new notification object: %s',
                            notification_object)
                return True
            except Exception as e:
                if is_duplicate_entry_error(e):
                    logger.info('Ignoring duplicate entry error')
                else:
                    logger.info('failed to add notification')
                    tx.rollback()
        return False

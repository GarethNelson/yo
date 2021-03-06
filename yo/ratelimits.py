# -*- coding: utf-8 -*-
import logging

from .db import Priority

logger = logging.getLogger(__name__)


# pylint: disable=too-many-branches
# pylint: disable-msg=unused-argument
def check_ratelimit(db, notification_object, override=False):
    """Checks if this notification should be sent or not

    Args:
        notification_object(dict): The notification in question, must contain the priority field (priority_level)
    Keyword args:
        override(bool): If set True, will use the hard limits instead of soft

    Returns:
        True if allowed, False if not
        :param notification_object:
        :param override:
        :param db:
    """

    notification_priority = notification_object['priority_level']
    to_username = notification_object['to_username']


    if notification_priority == Priority.ALWAYS:
        logger.debug('PRIORITY ALWAYS Checking')
        return db.get_priority_count(to_username, Priority.ALWAYS, 3600) < 10
    elif notification_priority == Priority.PRIORITY:
        logger.debug('PRIORITY PRIORITY Checking')
        if override:
            return (db.get_priority_count(to_username, Priority.PRIORITY, 3600)
                    <= 10)
        else:
            return (db.get_priority_count(to_username, Priority.PRIORITY,
                                          3600) <= 1)
    elif notification_priority == Priority.NORMAL:
        logger.debug('PRIORITY PRIORITY Checking')
        if override:
            return (db.get_priority_count(to_username, Priority.NORMAL, 60) <
                    3)
        else:
            return (db.get_priority_count(to_username, Priority.NORMAL,
                                          60) <= 1)
    elif notification_priority == Priority.LOW:
        logger.debug('PRIORITY LOW Checking')
        count = db.get_priority_count(to_username, int(Priority.LOW), 3600)
        logger.debug('Found %d existing objects of LOW priority for username %s', count,to_username)
        if override:
            return (db.get_priority_count(to_username, Priority.LOW, 3600) <
                    10)
        else:
            return (db.get_priority_count(to_username, Priority.LOW,
                                          3600) == 0)
    elif notification_priority == Priority.MARKETING:
        logger.debug('PRIORITY MARKETING Checking')
        if override:
            return (db.get_priority_count(to_username, Priority.MARKETING,
                                          86400) <= 1)
        else:
            return (db.get_priority_count(to_username, Priority.MARKETING,
                                          3600) <= 1)
    else:
        logger.error(
            'Invalid notification priority level! Assuming corrupted data for notification: %s',
            notification_object)

    return False  # for invalid stuff, assume it's bad

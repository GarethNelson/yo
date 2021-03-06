# -*- coding: utf-8 -*-
import json
import logging

from ..ratelimits import check_ratelimit
from ..transports import sendgrid
from ..transports import twilio
from ..transports import wwwpoll
from .base_service import YoBaseService

logger = logging.getLogger(__name__)
""" Basic design:

     1. Blockchain sender inserts notification into DB
     2. Blockchain sender triggers the notification by calling internal API method
     3. Notification sender checks if the notification is already sent or not, if not it sends to all configured transports and updates it to sent
"""


class YoNotificationSender(YoBaseService):
    service_name = 'notification_sender'

    def __init__(self, yo_app=None, config=None, db=None):
        super().__init__(yo_app=yo_app, config=config, db=db)
        self.configured_transports = {}

    async def api_trigger_notifications(self):
        await self.run_send_notify()
        return {'result': 'Succeeded'}  # FIXME

    async def run_send_notify(self):
        unsents = self.db.get_wwwpoll_unsents()
        logger.debug('run_send_notify() handling %s unsents', str(len(unsents)))

        for username, notifications in unsents.items():
            logger.info(
                'run_send_notify() handling user %s with %d notifications',
                username, len(notifications))
            user_transports = self.db.get_user_transports(username)
            user_notify_types_transports = {}
            for transport_name, transport_data in user_transports.items():
                for notify_type in transport_data['notification_types']:
                    if notify_type not in user_notify_types_transports.keys():
                        user_notify_types_transports[notify_type] = []
                    user_notify_types_transports[notify_type].append(
                        (transport_name, transport_data['sub_data']))
            for notification in notifications:
                logger.debug('Ratelimit checking on %s', str(notification))
                if not check_ratelimit(self.db, notification):
                    logger.info(
                        'Skipping notification for failing rate limit check: %s',
                        str(notification))
                    continue
                for t in user_notify_types_transports[notification[
                        'notify_type']]:
                    logger.info('Sending notification %s to transport %s',
                                str(notification), str(t[0]))
                    try:
                       self.configured_transports[t[0]].send_notification(
                            to_subdata=t[1],
                            to_username=username,
                            notify_type=notification['notify_type'],
                            data=json.loads(notification['json_data']))
                       self.db.mark_sent(notification,t[0])
                    except:
                       logger.exception('Exception occurred when sending notification %s', str(notification))

    def init_api(self):
        self.private_api_methods[
            'trigger_notifications'] = self.api_trigger_notifications
        if self.yo_app.config.config_data['wwwpoll'].getint('enabled', 1):
            logger.info('Enabling wwwpoll transport')
            self.configured_transports['wwwpoll'] = wwwpoll.WWWPollTransport(
                self.db)
        if self.yo_app.config.config_data['sendgrid'].getint('enabled', 0):
            logger.info('Enabling sendgrid (email) transport')
            self.configured_transports['email'] = sendgrid.SendGridTransport(
                self.yo_app.config.config_data['sendgrid']['priv_key'],
                self.yo_app.config.config_data['sendgrid']['templates_dir'])
        if self.yo_app.config.config_data['twilio'].getint('enabled', 0):
            logger.info('Enabling twilio (sms) transport')
            self.configured_transports['sms'] = twilio.TwilioTransport(
                self.yo_app.config.config_data['twilio']['account_sid'],
                self.yo_app.config.config_data['twilio']['auth_token'],
                self.yo_app.config.config_data['twilio']['from_number'],
            )

    async def async_task(self):
        pass

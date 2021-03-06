# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import re

import datetime

import uuid

import steem
from steem.blockchain import Blockchain

from ..db import Priority
from .base_service import YoBaseService

logger = logging.getLogger(__name__)

# TODO - use reliable stream when merged into steem-python

# Basically this service just follows the blockchain and inserts into the
# DB then triggers the notification sender to send the actual notification

# NOTIFICATION TYPES
ACCOUNT_UPDATE = 'account_update'
# Not a blockchain event ANNOUNCEMENT_IMPORTANT = 'announcement_important'
COMMENT_REPLY = 'comment_reply'
FEED = 'feed'
FOLLOW = 'follow'
MENTION = 'mention'
POST_REPLY = 'post_reply'
POWER_DOWN = 'power_down'
SEND_STEEM = 'send'
RECEIVE_STEEM = 'receive'
RESTEEM = 'resteem'
REWARD = 'reward'
VOTE = 'vote'

# any valid @username with a trailing whitespace
MENTION_PATTERN = re.compile(r'@([a-z][a-z0-9\-]{2,15})\s')


class YoBlockchainFollower(YoBaseService):
    service_name = 'blockchain_follower'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        steemd_url = self.yo_app.config.config_data['blockchain_follower'].get(
            'steemd_url', 'https://api.steemit.com')
        self.steemd_rpc = steem.steemd.Steemd(nodes=[steemd_url])
        self.follower_id = str(uuid.uuid1())

    async def store_notification(self, **data):
        data['sent'] = False
        self.db.create_notification(**data)

    async def handle_vote(self, op):
        logger.info('handle_vote received %s op', ['op'][0])
        vote_info = op['op'][1]
        logger.info('Vote on %s (written by %s) by %s with weight %s',
                    vote_info['permlink'], vote_info['author'],
                    vote_info['voter'], vote_info['weight'])
        await self.store_notification(
            trx_id=op['trx_id'],
            from_username=vote_info['voter'],
            to_username=vote_info['author'],
            json_data=json.dumps(vote_info),
            notify_type=VOTE,
            priority_level=int(Priority.LOW))
        return True

    async def handle_follow(self, op):
        op_data = op['op'][1]
        follow_data = json.loads(op_data['json'])
        if follow_data[0] != 'follow':
            return False
        follower = follow_data[1]['follower']
        following = follow_data[1]['following']
        if len(op_data['required_posting_auths']) != 1:
            logger.error('inavlid follow op, got %d posting auths, expected 1',
                         op_data['required_posting_auths'])
            return False
        if op_data['required_posting_auths'][0] != follower:
            logger.error('invalid follow op, follower must be signer')
            return False
        logger.debug('Follow: %s started following %s', follower, following)
        await self.store_notification(
            trx_id=op['trx_id'],
            from_username=follower,
            to_username=following,
            json_data=json.dumps(follow_data[1]),
            notify_type=FOLLOW,
            priority_level=Priority.LOW)
        return True

    async def handle_account_update(self, op):
        op_data = op['op'][1]
        logger.debug('Account: %s updated their account info',
                     op_data['account'])
        await self.store_notification(
            trx_id=op['trx_id'],
            to_username=op_data['account'],
            json_data=json.dumps(op_data),
            notify_type=ACCOUNT_UPDATE,
            priority_level=Priority.LOW)
        return True

    async def handle_send(self, op):
        op_data = op['op'][1]
        send_data = {
            'amount': op_data['amount'],
            'from': op_data['from'],
            'memo': op_data['memo'],
            'to': op_data['to'],
        }
        logger.debug('Send: %s sent %s to %s', send_data['from'],
                     send_data['amount'], send_data['to'])
        await self.store_notification(
            trx_id=op['trx_id'],
            to_username=send_data['from'],
            json_data=json.dumps(send_data),
            notify_type=SEND_STEEM,
            priority_level=Priority.LOW)
        return True

    async def handle_receive(self, op):
        op_data = op['op'][1]
        receive_data = {
            'amount': op_data['amount'],
            'from': op_data['from'],
            'memo': op_data['memo'],
            'to': op_data['to'],
        }
        logger.debug('Receive: %s got %s from %s', receive_data['to'],
                     receive_data['amount'], receive_data['from'])
        await self.store_notification(
            trx_id=op['trx_id'],
            to_username=receive_data['to'],
            from_username=receive_data['from'],
            json_data=json.dumps(receive_data),
            notify_type=RECEIVE_STEEM,
            priority_level=Priority.LOW)
        return True

    async def handle_power_down(self, op):
        op_data = op['op'][1]
        logger.debug('Powerdown: %s powered down %s', op_data['account'],
                     op_data['vesting_shares'])
        await self.store_notification(
            trx_id=op['trx_id'],
            to_username=op_data['account'],
            json_data=json.dumps(op_data),
            notify_type=POWER_DOWN,
            priority_level=Priority.LOW)
        return True

    async def handle_mention(self, op):
        comment_data = op['op'][1]
        haystack = comment_data['body'] + '\n'
        data = {
            'author': comment_data['author'],
            'permlink': comment_data['permlink'],
        }
        for match in re.findall(MENTION_PATTERN, haystack):
            # TODO: only allow N mentions per operation?
            # TODO: validate mentioned user exists on chain?
            logger.debug('Mention: %s mentioned %s', data['author'], match)
            await self.store_notification(
                trx_id=op['trx_id'],
                to_username=match,
                from_username=data['author'],
                json_data=json.dumps(data),
                notify_type=MENTION,
                priority_level=Priority.LOW)
        return True

    async def handle_comment(self, op):
        logger.debug('handle_comment recevied %s op', ['op'][0])
        op_data = op['op'][1]
        if op_data['parent_author'] == '':
            # top level post
            return True
        parent_id = '@' + op_data['parent_author'] + '/' + op_data['parent_permlink']
        parent = steem.post.Post(parent_id)
        note_type = COMMENT_REPLY if parent.is_comment() else POST_REPLY
        logger.debug('Comment(%s): %s replied to %s', note_type,
                     op_data['author'], parent_id)
        await self.store_notification(
            trx_id=op['trx_id'],
            to_username=op_data['parent_author'],
            from_username=op_data['author'],
            json_data=json.dumps(op_data),
            notify_type=note_type,
            priority_level=Priority.LOW)
        return True

    async def handle_resteem(self, op):
        op_data = op['op'][1]
        resteem_data = json.loads(op_data['json'])
        if resteem_data[0] != 'reblog':
            return True
        account = resteem_data[1]['account']
        author = resteem_data[1]['author']
        permlink = resteem_data[1]['permlink']
        if len(op_data['required_posting_auths']) != 1:
            logger.error(
                'inavlid resteem op, got %d posting auths, expected 1',
                op_data['required_posting_auths'])
            return True
        if op_data['required_posting_auths'][0] != account:
            logger.error('invalid resteem op, account must be signer')
            return True
        logger.debug('Resteem: %s reblogged @%s/%s', account, author, permlink)
        await self.store_notification(
            trx_id=op['trx_id'],
            from_username=account,
            to_username=author,
            json_data=json.dumps(resteem_data[1]),
            notify_type=RESTEEM,
            priority_level=Priority.LOW)
        return True

    async def notify(self, blockchain_op):
        """ Handle notification for a particular op
        """
        logger.debug('Got operation from blockchain: %s', str(blockchain_op))
        # vote
        if blockchain_op['op'][0] == 'vote':
            return await self.handle_vote(blockchain_op)
            # handle notifications for upvotes here based on user preferences in DB

        # follow, resteem
        elif blockchain_op['op'][0] == 'custom_json':
            if blockchain_op['op'][1]['id'] == 'follow':
                logger.debug('Incoming custom_json operation')
                # handle follow notifications here
                return await asyncio.gather(
                    self.handle_follow(blockchain_op),
                    self.handle_resteem(blockchain_op))

        # account_update
        elif blockchain_op['op'][0] == 'account_update':
            logger.debug('Incoming account_update operation')
            return await self.handle_account_update(blockchain_op)

        # send, receive
        elif blockchain_op['op'][0] == 'transfer':
            logger.debug('Incoming transfer operation')
            return await asyncio.gather(
                self.handle_send(blockchain_op),
                self.handle_receive(blockchain_op))

        # power_down
        elif blockchain_op['op'][0] == 'withdraw_vesting':
            logger.debug('Incoming withdraw_vesting operation')
            return await self.handle_power_down(blockchain_op)

        # mention, comment-reply, post-reply
        elif blockchain_op['op'][0] == 'comment':
            logger.debug('Incoming comment operation')
            return await asyncio.gather(
                self.handle_mention(blockchain_op),
                self.handle_comment(blockchain_op))

        # reward
        # feed
        return True  # return this or the op will be requeued

    async def run_queue(self, q):
        while not q.empty():
            op = await q.get()

            resp = await self.notify(op)
            if not resp:
                logger.debug('Re-queueing operation: %s', str(op))
                return op
        return None

    def get_start_block(self,b):
        """ Gets the starting block from configuration
        """

        start_block = str(
            self.yo_app.config.config_data['blockchain_follower'].get(
                'start_block', ''))
        # turn the start_block into something understandable to steem-python:
        # blank value is None
        # negative values are the head block minus that amount
        if start_block == '':
            start_block = None  # TODO: at some point go back and implement the block_status thing for multiple blockchain followers
        else:
            start_block = int(
                start_block
            )  # TODO: handle malformed config in the config module and spit out appropriate errors
            if start_block < 0:
                start_block = b.get_current_block_num() - start_block
        return start_block

    async def async_task(self):

        logger.info('Blockchain follower started')
        chain = Blockchain(steemd_instance=self.steemd_rpc)
        start_block = self.get_start_block(chain)
        block_interval = chain.config().get("STEEMIT_BLOCK_INTERVAL") # we use this to calculate timeouts

        while True:
           chain_status = self.db.get_chain_status()

           if chain_status is None: # we must be the first, so let's init stuff
              chain_status = self.db.try_active_follower(follower_id=self.follower_id,last_processed_block=start_block-1,lock_timeout=block_interval*10) # timeout after 10 blocks go unprocessed
           else: # an existing follower is in the DB, check if it's expired, and if so take over
              chain_status = self.db.try_active_follower(follower_id=self.follower_id,lock_timeout=block_interval*10) # we don't overwrite last_processed_block

           if chain_status['active_follower_id'] == self.follower_id: # we are active
              await self.run_active(chain=chain,start_block=chain_status['last_processed_block']+1,max_blocks=10,block_interval=block_interval)
           else: # we are not active, so go to sleep for now
              logger.info('We are not active follower, sleeping until %s', str(chain_status['lock_expires']))
              now = datetime.datetime.now()
              sleep_time = chain_status['lock_expires'] - now
              await asyncio.sleep(sleep_time.total_seconds())


    async def run_active(self,chain=None,start_block=None,max_blocks=9,block_interval=2):
          queue = asyncio.Queue()
          logger.debug('We are active follower!')
          if start_block is None: start_block = self.get_start_block(chain)
          processed_count = 0

          for block_num in range(start_block,start_block+max_blocks):
              try:
                 ops = self.steemd_rpc.get_ops_in_block(block_num, False)
                 for op in ops:
                       await queue.put(op)
                       await asyncio.sleep(0)
                       runner_resp = await self.run_queue(queue)
                       if runner_resp:
                          queue.put(runner_resp)
                 processed_count += 1
                 new_timeout = ((max_blocks+1) - processed_count) * block_interval # as we process more blocks, shrink our timeout, but leave enough space for another block
                 self.db.try_update_status(follower_id = self.follower_id, last_processed_block = block_num, lock_timeout = new_timeout)
              except Exception:
                 logger.exception('Exception occurred')

    def init_api(self):
        pass

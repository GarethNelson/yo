"""Full flow tests (blockchain op to transport)

   Basic idea is to setup yo's different components but not follow the real blockchain

"""


import pytest


import json
import uuid
from yo.services import blockchain_follower
from yo.services import notification_sender
from yo.services import api_server
from yo import config
from yo import ratelimits
from yo.transports import base_transport
from yo.db import Priority


@pytest.fixture(autouse=True)
def add_mock_transport_type(monkeypatch):
    from yo.db import TRANSPORT_TYPES
    transport_types_set = set(TRANSPORT_TYPES)
    transport_types_set.add('mock')
    monkeypatch.setattr(api_server,'TRANSPORT_TYPES',transport_types_set)

class MockApp:
   def __init__(self,db):
       self.db = db
       self.config = config.YoConfigManager(None)


class MockTransport(base_transport.BaseTransport):
   def __init__(self):
       self.received_by_user = {}
       self.rxcount = 0
   def send_notification(self,to_subdata=None,to_username=None,notify_type=None,data=None):
       print((to_subdata,to_username,notify_type,data))
       self.received_by_user[to_username] = (to_subdata,notify_type,data)
       self.rxcount += 1

def gen_vote_op():
    return  {'trx_id':str(uuid.uuid4()),
                 'op':('vote',{'permlink':'test-post',
                               'author'  :'testupvoted',
                               'voter'   :'testupvoter',
                               'weight'  :10000})}

@pytest.mark.asyncio
async def test_basic_ratelimit(sqlite_db):
    """Tests basic ratelimit functionality
    """

    # boilerplate stuff
    yo_db    = sqlite_db    
    yo_app   = MockApp(yo_db)
    sender   = notification_sender.YoNotificationSender(db=yo_db,yo_app=yo_app)
    mock_tx  = MockTransport()
    sender.configured_transports = {}
    sender.configured_transports['mock'] = mock_tx
    API      = api_server.YoAPIServer()
    follower = blockchain_follower.YoBlockchainFollower(db=yo_db,yo_app=yo_app)

    # configure testupvoted and testupvoter users to use mock transport for votes
    transports_obj = {'mock':{'notification_types':['vote'],'sub_data':''}}
    await API.api_set_transports(username='testupvoted',transports=transports_obj,context=dict(yo_db=sqlite_db))
    await API.api_set_transports(username='testupvoter',transports=transports_obj,context=dict(yo_db=sqlite_db))

    # send a single vote op
    vote_op = gen_vote_op()
    await follower.notify(vote_op)
    await sender.api_trigger_notifications()

    # ensure single vote op got through (should be priority LOW)
    assert 'testupvoted' in mock_tx.received_by_user.keys()
    assert mock_tx.rxcount == 1

    # immediately attempt to send a second vote op, this one should fail
    vote_op = gen_vote_op()
    await follower.notify(vote_op)
    await sender.api_trigger_notifications()

    # ensure there is still only one vote op sent
    assert mock_tx.rxcount == 1

    # check that the ratelimit fails for a fresh vote op
    vote_op = gen_vote_op()
    vote_op['priority_level'] = Priority.LOW # some silly hacks as we've got a raw blockchain op here
    vote_op['to_username']    = 'testupvoted'
    assert not ratelimits.check_ratelimit(yo_db,vote_op)

    # check the ratelimit succeeds when override flag is set
    assert ratelimits.check_ratelimit(yo_db,vote_op, override=True)

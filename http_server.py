#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import gevent, gevent.pywsgi, gevent.hub
from arago.actors.monitor import RESUME
from arago.actors import Actor, ConsistentHashingRouter, OnDemandRouter
import falcon
import json
import sys, traceback, logging

from arago.common.logging import getCustomLogger

logger = getCustomLogger(level="DEBUG", logfile=sys.stderr)

def print_exception(context, type, value, tb):
	logger = logging.getLogger('root')
	logger.trace("Gevent Hub:\n" + "".join(traceback.format_exception(type, value, tb)))

gevent.hub.get_hub().print_exception = print_exception

class Echo(Actor):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
	def handle(self, task):
		#gevent.sleep(1)
		if json.loads(task.msg)['payload'] == "kill":
			return result
		elif json.loads(task.msg)['payload'] == "die":
			self.shutdown()
			return "You bastards!"
		else:
			return "{name} says {greeting}".format(
				name=self.name,
				greeting=json.loads(task.msg)['payload'])

class Producer(object):
	def __init__(self, handler):
		self._handler=handler
	def on_post(self, req, resp):
		resp.body=self._handler.await(req.bounded_stream.read())

#workers = [Echo(name="echo-{num}".format(num=i)) for i in range(10)]

#router = ConsistentHashingRouter(mapfunc=lambda msg: msg[0], children=workers)

router = OnDemandRouter(
	Echo, worker_name_tpl="echo",
	mapfunc=lambda msg: json.loads(msg)['target'],
	policy=RESUME
)

router.link(lambda child: child.resume())

producer = Producer(router)

app = falcon.API()
app.add_route('/', producer)
server = gevent.pywsgi.WSGIServer(('127.0.0.1', 8080), app, log=gevent.pywsgi.LoggingLogAdapter(logger, level=logger.VERBOSE), error_log=logger)
print("ready")
server.serve_forever()

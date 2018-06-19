#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import gevent, gevent.pywsgi, gevent.hub
from arago.actors import Task, Actor, Monitor, RESUME, RESTART, IGNORE, SHUTDOWN, ActorStoppedError
from arago.actors.routers.on_demand import OnDemandRouter
import falcon, json, sys, traceback, logging
from gevent.backdoor import BackdoorServer
from arago.common.logging import getCustomLogger
import function_pattern_matching as fpm

logger = getCustomLogger(
	level="TRACE", logfile=sys.stderr,
	#formatting=("%(asctime)s %(levelname)-7s %(message)s in %(pathname)s:%(lineno)s", "%Y-%m-%d %H:%M:%S")
	formatting=("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
)

def print_exception(context, type, value, tb):
	logger = logging.getLogger('root')
	fill = 4 * " "
	tb = ("\n" + fill).join(("".join(traceback.format_exception(type, value, tb))).splitlines())
	logger.trace("Gevent Hub:\n{f}{tb}".format(f=fill, tb=tb)
	)

gevent.hub.get_hub().print_exception = print_exception

def payload(*values):
	def wrapper(msg):
		try:
			return json.loads(msg)['payload'] in values if values else True
		except (KeyError, json.decoder.JSONDecodeError):
			return False
	return fpm.GuardFunc(wrapper)

def default_match(*args, **kwargs):
	return fpm.case(*args, **kwargs)

def match(*args, **kwargs):
	return fpm.case(fpm.guard(*args, **kwargs))


class Echo(Actor):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	@match
	def handle(self, msg: payload("crash"), sender):
		"""Provoke a crash by referring a non-existent name"""
		return result

	@match
	def handle(self, msg: payload("die"), sender):
		"""Stop yourself"""
		self.stop()
		return "You bastards!"

	@match
	def handle(self, msg: payload("kill"), sender):
		"""Clear your own mailbox and context"""
		self.clear()
		return "I can't remember what I wanted to do next."

	@match
	def handle(self, msg: payload(), sender):
		"""Send the payload back to the sender"""
		return "{name} says {greeting}".format(name=self.name, greeting=json.loads(msg)['payload'])

	@default_match
	def handle(self, msg, sender):
		"""Message is missing the payload"""
		raise falcon.HTTPBadRequest()

class Producer(object):
	def __init__(self, handler):
		self._handler=handler
	def on_post(self, req, resp):
		resp.body=self._handler.await(req.bounded_stream.read(), timeout=None, retry=10)

router = OnDemandRouter(
	Echo, name="router-1", worker_name_tpl="echo",
	mapfunc=lambda msg: json.loads(msg)['target'],
	policy=SHUTDOWN, max_idle=10
)

monitor = Monitor(name="monitor-1", children=[router], policy=RESUME)

producer = Producer(router)

bdserver = BackdoorServer(
	('127.0.0.1', 5001),
	banner="Hello from gevent backdoor!",
	locals={'root': monitor, 'input': producer}
)
bd=gevent.spawn(bdserver.serve_forever)

app = falcon.API()
app.add_route('/', producer)
server = gevent.pywsgi.WSGIServer(
	('127.0.0.1', 8080),
	app,
	log=gevent.pywsgi.LoggingLogAdapter(logger, level=logger.VERBOSE),
	error_log=logger
)
logger.info("REST ready")
server.serve_forever()

#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import gevent, gevent.pywsgi, gevent.hub
from arago.actors import Task, Actor, Monitor, RESUME, RESTART, IGNORE, SHUTDOWN, ActorStoppedError
from arago.actors.routers.on_demand import OnDemandRouter
import falcon, json, sys, traceback, logging
from gevent.backdoor import BackdoorServer
from arago.common.logging import getCustomLogger
import function_pattern_matching as fpm
import hashlib

logger = getCustomLogger(
	level="TRACE", logfile=sys.stderr,
	#formatting=("%(asctime)s %(levelname)-7s %(message)s in %(pathname)s:%(lineno)s", "%Y-%m-%d %H:%M:%S")
	formatting=("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
)

def print_exception(context, type, value, tb):
	logger = logging.getLogger('root')
	fill = 4 * " "
	tb = ("\n" + fill).join(("".join(traceback.format_exception(type, value, tb))).splitlines())
	logger.trace("Gevent Hub:\n{f}{tb}".format(f=fill, tb=tb))

gevent.hub.get_hub().print_exception = print_exception

def command(*values):
	def wrapper(task):
		command, payload = task.msg
		return command in values if values else True
	return fpm.GuardFunc(wrapper)

def default_match(*args, **kwargs): return fpm.case(*args, **kwargs)

def match(*args, **kwargs): return fpm.case(fpm.guard(*args, **kwargs))


class Anonymizer(Actor):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._rainbow = dict()

	@match
	def handle(self, task: command("hash")):
		"""Provoke a crash by referring a non-existent name"""
		command, payload = task.msg
		myhash = hashlib.sha256(payload).hexdigest()
		self._rainbow[myhash] = payload
		return myhash

	@match
	def handle(self, task: command("unhash")):
		"""Provoke a crash by referring a non-existent name"""
		command, payload = task.msg
		try:
			return self._rainbow[payload]
		except KeyError:
			self._logger.warn('Hash not found')
			raise falcon.HTTPNotFound()

	@default_match
	def handle(self, task):
		raise falcon.HTTPBadRequest()

class UnsafeEndpoint(object):
	def __init__(self, handler):
		self._handler=handler
	def on_post(self, req, resp):
		resp.body=self._handler.await(("hash" , req.bounded_stream.read()), timeout=None, retry=10)

class SafeEndpoint(object):
	def __init__(self, handler):
		self._handler=handler
	def on_post(self, req, resp):
		resp.body=self._handler.await(("unhash", req.bounded_stream.read().decode('utf-8')), timeout=None, retry=10)

anonymizer = Anonymizer(name="Geheim")

unsafe = UnsafeEndpoint(anonymizer)
safe = SafeEndpoint(anonymizer)

monitor = Monitor(name="monitor-1", children=[anonymizer], policy=RESUME)

app1 = falcon.API()
app1.add_route('/in', unsafe)
app2 = falcon.API()
app2.add_route('/out', safe)

server1 = gevent.pywsgi.WSGIServer(
	('127.0.0.1', 8080),
	app1,
	log=gevent.pywsgi.LoggingLogAdapter(logger, level=logger.VERBOSE),
	error_log=logger
)
server2 = gevent.pywsgi.WSGIServer(
	('127.0.0.1', 8081),
	app2,
	log=gevent.pywsgi.LoggingLogAdapter(logger, level=logger.VERBOSE),
	error_log=logger
)
servers = [gevent.spawn(srv.serve_forever) for srv in [server1, server2]]

logger.info("REST ready")
gevent.joinall(servers)

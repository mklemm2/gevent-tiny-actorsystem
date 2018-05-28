#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import gevent, gevent.pywsgi, gevent.hub
from arago.actors import Actor, Monitor, RESUME, RESTART, IGNORE, SHUTDOWN, ActorShutdownError, ActorStoppedError
from arago.actors.routers.on_demand import OnDemandRouter
import falcon
import json
import sys, traceback, logging
from gevent.backdoor import BackdoorServer

from arago.common.logging import getCustomLogger

logger = getCustomLogger(
	level="INFO", logfile=sys.stderr,
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

class Echo(Actor):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
	def handle(self, task):
		if json.loads(task.msg)['payload'] == "crash":
			return result
		elif json.loads(task.msg)['payload'] == "die":
			self.shutdown()
			return "You bastards!"
		else:
			return "{name} says {greeting}".format(name=self.name, greeting=json.loads(task.msg)['payload'])

class Producer(object):
	def __init__(self, handler):
		self._handler=handler
	def on_post(self, req, resp):
		resp.body=self._handler.await(req.bounded_stream.read(), timeout=None, retry=10)

router = OnDemandRouter(
	Echo, name="Mainspot", worker_name_tpl="echo",
	mapfunc=lambda msg: json.loads(msg)['target'],
	policy=SHUTDOWN, max_idle=30
)

monitor = Monitor(name="monitor-1", children=[router], policy=RESTART)

producer = Producer(router)

bdserver = BackdoorServer(('127.0.0.1', 5001),
                        banner="Hello from gevent backdoor!",
                        locals={'root': monitor, 'input': producer})
bd=gevent.spawn(bdserver.serve_forever)

app = falcon.API()
app.add_route('/', producer)
server = gevent.pywsgi.WSGIServer(('127.0.0.1', 8080), app, log=gevent.pywsgi.LoggingLogAdapter(logger, level=logger.VERBOSE), error_log=logger)
logger.info("REST ready")
server.serve_forever()

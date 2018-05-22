#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import gevent, gevent.pywsgi
from arago.actors import Actor, ConsistentHashingRouter
import falcon

class Echo(Actor):
	def handle(self, task):
		#gevent.sleep(1)
		return "{name} says {greeting}".format(
			name=self.name,
			greeting=task.msg)

class Producer(object):
	def __init__(self, handler):
		self._handler=handler
	def on_post(self, req, resp):
		resp.body=self._handler.await(req.bounded_stream.read())

workers = [Echo(name="echo-{num}".format(num=i)) for i in range(10)]

router = ConsistentHashingRouter(mapfunc=lambda msg: msg[0], children=workers)
producer = Producer(router)

app = falcon.API()
app.add_route('/', producer)
server = gevent.pywsgi.WSGIServer(('127.0.0.1', 8080), app)
print("ready")
server.serve_forever()

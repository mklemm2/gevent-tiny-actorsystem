#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
from arago.actors import Actor, Monitor, Root, RESUME
import arago.actors.pattern_matching as matching
from arago.common.logging import getCustomLogger
import gevent
import random

logger = getCustomLogger(level="DEBUG")

class Echo(Actor):
	@matching.match(msg = "crash")
	def handle(self, msg, payload, sender): return undefined

	@matching.match(msg = "stop")
	def handle(self, msg, payload, sender): self.stop()

	@matching.match(msg = matching.isoftype(str))
	def handle(self, msg, payload, sender): return "{me} replies: {msg}".format(me=self, msg=msg)

def send(target):
	for message in random.choices(["hello", 1], weights=[5,1], k=10):
		try:
			logger.info("Sending {msg} to {target}".format(msg=message, target=target))
			answer = target.wait_for(message)
			logger.info(answer)
		except Exception as e:
			logger.warning("Target raised an exception: {e}".format(e=e))

echo = Echo(name="echo")
monitor = Monitor(name="monitor", policy=RESUME, children=[echo])

gevent.spawn(send, echo).join()

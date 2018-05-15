#!/usr/bin/env python
from gevent import monkey; monkey.patch_all()
from arago.actors import Actor, Monitor, Router, Generator
from arago.actors import ActorShutdown, ActorTimeout, ActorCrash, ActorRestarted, RouterNoChildren
import gevent
import inspect
import logging
import random
import sys, os

logging.basicConfig(level=logging.DEBUG)

def mygen():
	while True:
		yield random.choice(['Hello', 'Hallo', 'crash'])

class Crash(Actor):
	def __init__(self, answer):
		super().__init__()
		self._answer = answer
	def handle(self, task):
		if task.msg == 'crash':
			raise ActorCrash("This is deliberate!")
		else:
			print("HANDLED!")
			return self._answer

def exit_on_crash(child):
	sys.exit(5)

workers = Router(policy='escalate', children=[Crash("42") for i in range(3)])
requester = Generator(mygen(), target=workers, method='await', processor=print, throttle=1)

main_loop = Monitor(policy='restart', children=[requester, workers])
main_loop.start()
requester.start()
main_loop.join()
print("the end")

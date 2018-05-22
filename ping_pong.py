#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import arago.actors.actor

class Ping(arago.actors.actor.Actor):
	def handle(self, task):
		print("{me} received {msg} from {other}.".format(me=self, msg=task.msg, other=task.sender))
		if task.msg == "Pong":
			task.sender.tell("Ping")
		elif task.msg == "start":
			self.opponent = Pong("PONG")
			self.opponent.tell("Ping")

class Pong(arago.actors.actor.Actor):
	def handle(self, task):
		print("{me} received {msg} from {other}.".format(me=self, msg=task.msg, other=task.sender))
		if task.msg == "Ping":
			task.sender.tell("Pong")

ping = Ping("PING")

ping.tell("start")
ping.join()

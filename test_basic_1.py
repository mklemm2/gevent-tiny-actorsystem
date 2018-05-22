#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import arago.actors.actor

class Echo(arago.actors.actor.Actor):
	def handle(self, task):
		return task.msg

class Crash(arago.actors.actor.Actor):
	def handle(self, task):
		if task.msg == "crash":
			raise Exception("Actor crashed")
		else:
			return "No Crash"

echo = Echo()
print(echo.await("Hello"))
crash = Crash()
try:
	print(crash.await("crash"))
except Exception as e:
	print(e)
x = crash.ask("no crash")
print("resuming")
crash.resume()
print(crash.await("no crash"))
print(x.get())

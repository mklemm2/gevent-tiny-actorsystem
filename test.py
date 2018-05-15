#!/usr/bin/env python
from gevent import monkey; monkey.patch_all()
from arago.actors import Actor, ActorShutdown, ActorTimeout
import gevent
import inspect
import logging

logging.basicConfig(level=logging.DEBUG)

class Echo(Actor):
	def __init__(self, meinarg):
		super().__init__()
		self._restart_state = ((meinarg, ), {})
		self.mein = meinarg

	def handle(self, task):
		if task.msg == "Hello":
			print("Handled: " + task.msg)
			return task.msg
		if task.msg == "Hallo":
			print("Asking myself: " + task.msg)
			return self.await("Hello")
		elif task.msg == "Hallihallo":
			gevent.sleep(5)
			print("Handled: " + task.msg)
			return task.msg
		elif task.msg == "Hallihallohalloechen":
			gevent.sleep(5)
			print("Handled: " + task.msg)
			return task.msg

def test1(a, msg, i):
	while True:
		try:
			a.tell(msg)
			gevent.sleep(i)
		except ActorShutdown:
			print("I didn't get the memo!")
			try:
				a = a.get_current_address(timeout=5)
				print("Got new address!")
			except ActorTimeout as e:
				print(e)
				print("No replacement, exiting")
				break

tom = Echo("test")
print(tom.__class__)
tom.start()
print(tom.__dict__)
tom.spawn_child(Echo, "toast")
main_loop = gevent.spawn(test1, tom, "Hello", 1)
gevent.idle()
print("awaiting manually")
r = tom.ask("Hallihallo")
r.get()
print("awaiting done")
print("Answered: " + r.get())
print("Awaiting automatically")
print("Awaited: " + tom.await("Hallihallohalloechen"))
print("awaiting done")
#print(r.get())
gevent.sleep(3)
tom.tell("Hallihallo")
print("Shutting down " + str(tom))
tom.shutdown()
print("Trying to ask the dying fellow about the ring ...")
try:
	tom.ask("Hallihallo")
except ActorShutdown as e:
	print(e)
print("Well, he's not talking, anymore")
gevent.sleep(3)
print("Wait for him to die")
try:
	tom.join()
except ActorShutdown as e:
	print(e)
print("He died")
print("resurrection!")
print(tom)
tom = tom.restart()
print(tom)
tom.tell("Hallo")
gevent.sleep(5)
tom.shutdown()
main_loop.join()

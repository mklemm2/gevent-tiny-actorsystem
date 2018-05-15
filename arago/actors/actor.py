import gevent, gevent.pool, gevent.event
import logging
from gevent.greenlet import Greenlet
from greenlet import GreenletExit
from gevent.queue import JoinableQueue
from greenlet import greenlet
from gevent.hub import get_hub
import random
import traceback

class Task(gevent.event.AsyncResult):
	def __init__(self, msg, sender=None):
		super().__init__()
		self.msg = msg
		self._sender = sender

class ActorShutdown(Exception):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class ActorCrash(Exception):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class ActorRestarted(Exception):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class ActorTimeout(Exception):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class ActorNotRestartable(Exception):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class RouterNoChildren(Exception):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

class ExitResult(gevent.event.AsyncResult):
	pass

class Actor(Greenlet):
	def __new__(cls, *args, **kwargs):
		obj = Greenlet.__new__(cls, *args, **kwargs)
		obj._restart_args = args
		obj._restart_kwargs = kwargs
		return obj

	def __init__(self, max_children=None, logger=None):
		super().__init__()
		self._logger = logger or logging.getLogger("root")
		self._restart = gevent.event.Event()
		self._mailbox = JoinableQueue()
		self._children = (gevent.pool.Pool(size=max_children)
		                  if max_children
		                  else gevent.pool.Group())
		self._shutdown = gevent.event.Event()
		self._shutdown_exc = ActorShutdown("{act} is being shutdown...".format(act=self))
		self._logger.debug("{act} created.".format(act=self))
		self.start()

	def update_state(self, old, new):
		self._restart_args = tuple([new if item == old else item for item in self._restart_args])
		self._restart_kwargs = {k:new if v == old else v for k,v in self._restart_kwargs.items()}

	def _run(self):
		for task in self._mailbox:
			if isinstance(task, Task):
				try:
					task.set(self.handle(task))
				except Exception as e:
					task.set_exception(e)
					raise
			elif isinstance(task, ExitResult):
				self._logger.debug("All pending tasks handled, {act} exiting...".format(act=self))
				self.kill()
			else:
				self._logger.warn("{act} received an invalid message: {msg}".format(act=self, msg=task))
			self._mailbox.task_done()

	def handle(self, message):
		raise NotImplementedError

	def receive(self, msg, sender):
		if not isinstance(sender, Actor):
			sender = None
		if self._shutdown.is_set() or self.dead:
			self._logger.debug("{act} does not accept any more requests")
			raise self._shutdown_exc
		task = Task(msg, sender)
		self._mailbox.put(task)
		return task

	def tell(self, msg):
		sender = gevent.getcurrent()
		self._logger.debug("{sen} TOLD {rec}: {msg}".format(rec=self, sen=sender, msg=msg))
		self.receive(msg, sender)

	def ask(self, msg):
		sender = gevent.getcurrent()
		self._logger.debug("{sen} ASKED {rec}: {msg}".format(rec=self, sen=sender, msg=msg))
		return self.receive(msg, sender)

	def await(self, msg):
		sender = gevent.getcurrent()
		self._logger.debug("{sen} is AWAITING {rec}: {msg}".format(rec=self, sen=sender, msg=msg))
		return self.receive(msg, sender).get()

	def restart(self):
		self._logger.debug("{act} will be restarted using the following arguments: *args={args}, **kwargs={kwargs}".format(act=self, args=self._restart_args, kwargs=self._restart_kwargs))
		self._replacement = self.__class__(*self._restart_args, **self._restart_kwargs)
		self._replacement.start()
		self._restart.set()
		return self._replacement

	def get_current_address(self, block=True, timeout=None):
		if block:
			if self._restart.wait(timeout):
				return self._replacement
			else:
				raise ActorTimeout("Actor.get_current_address() timed out")

	def shutdown(self, block=True):
		gevent.idle()
		self._logger.debug("{act} is shutting down.".format(act=self))
		self._shutdown.set()
		for child in self._children:
			child.shutdown()
		self._mailbox.put(ExitResult())
		if block:
			self._mailbox.join()

	def register_child(self, child):
		self._children.add(child)

	def spawn_child(self, cls, *args, **kwargs):
		self._children.start(cls(*args, **kwargs))

	def unregister_child(self, actor):
		self._children.discard(actor)

class Monitor(Actor):
	def __init__(self, policy='restart', max_children=None, logger=None, children=None):
		super().__init__(max_children=max_children, logger=logger)
		self._policy = policy
		if children:
			for child in children:
				self.register_child(child)

	def handle(self, task):
		pass

	def restart(self):
		if 'children' in self._restart_kwargs:
			self._logger.debug("restarting monitored children ...")
			self._restart_kwargs['children'] = [child.restart() for child in self._restart_kwargs['children']]
		self._logger.debug("restarting monitor ...")
		return super().restart()

	def _run(self):
		self.join()

	def handle_child_exit(self, child):
		if self._policy == 'restart':
			self._logger.debug("RESTART" + str(child))
			try:
				replacement = child.restart()
				replacement.link_exception(self.handle_child_exit)
				## UPDATE STATE
				self._children.add(replacement)
				self._logger.debug("{rt}: {new} replaces {old}".format(rt=self, new=replacement, old=child))
			except ActorNotRestartable as e:
				self._logger.error(e)
				self._logger.error("{act} failed to restart a child, escalating...".format(act=self))
				self.kill(ActorCrash("{act} crashed".format(act=self)))

		elif self._policy == 'escalate':
			self._logger.error("{child}, a child of {act} died, escalating...".format(act=self, child=child))
			self.kill(ActorCrash("{act} crashed".format(act=self)))

		elif self._policy == 'ignore':
			self._logger.error("{child}, a child of {act} died, ignoring...".format(act=self, child=child))

		elif self._policy == 'escalate_on_empty':
			if len(self._children.greenlets) > 1:
				self._logger.error("{child}, a child of {act} died, ignoring...".format(act=self, child=child))
			else:
				self._logger.error("{child}, the last child of {act} died, escalating...".format(act=self, child=child))
				self.kill(ActorCrash("{act} crashed".format(act=self)))

	def spawn_child(self, cls, *args, **kwargs):
		child = cls(*args, **kwargs)
		child.link_exception(self.handle_child_exit)
		self._children.start(child)

	def register_child(self, child):
		child.link_exception(self.handle_child_exit)
		self._children.add(child)

class Router(Monitor):
	def receive(self, msg, sender):
		if self._shutdown.is_set() or self.dead:
			self._logger.debug("{act} does not accept any more requests".format(act=self))
			if self._restart.is_set():
				raise ActorRestarted("{act} was replaced by {rep}".format(act=self, rep=self.get_current_address()))
			else:
				raise ActorCrash("{act} crashed".format(act=self))
		try:
			target = random.choice(tuple(self._children.greenlets))
			self._logger.debug("{rt} routed message '{msg}' to {target}".format(rt=self, msg=msg, target=target))
			return target.receive(msg, sender)
		except IndexError:
			raise RouterNoChildren("{rt} failed to route message '{msg}' from {sender}.".format(rt=self, msg=msg, sender=sender))

class Generator(Actor):
	def __init__(self, gen, target, method='tell', processor=None, throttle=None):
		super().__init__()
		self._gen = gen
		self._target = target
		self._method = method
		self._processor = processor
		self._throttle=throttle

	def _run(self):
		for item in self._gen:
			while True:
				try:
					method = getattr(self._target, self._method, None)
					if method and callable(method):
						r = method(item)
						if self._processor and callable(self._processor):
							self._processor(r)
						if self._throttle:
							gevent.sleep(self._throttle)
				except ActorRestarted as e:
					new_target = self._target.get_current_address()
					self.update_state(self._target, new_target)
					self._target = new_target
					continue
				except ActorCrash as e:
					self._logger.error("TARGET CRASHED, STOPPING")
					self.kill(ActorCrash("{act} crashed".format(act=self)))
				break

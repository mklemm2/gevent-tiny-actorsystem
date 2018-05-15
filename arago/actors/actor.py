import gevent, gevent.pool, gevent.event, gevent.greenlet, gevent.queue
import random, pickle

__shared_nothing__ = True


class ActorCrashedError(Exception):
	pass

class Actor(gevent.greenlet.Greenlet):
	class Task(gevent.event.AsyncResult):
		def __init__(self, msg, sender=None):
			super().__init__()
			self._msg = (pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
						 if __shared_nothing__ else msg)
			self.sender = sender

		@property
		def msg(self):
			return pickle.loads(self._msg) if __shared_nothing__ else self._msg

		def set(self, result):
			return super().set(pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)
							   if __shared_nothing__ else result)

		def get(self):
			return pickle.loads(super().get()) if __shared_nothing__ else super().get()

	def __init__(self):
		super().__init__()
		self._mailbox = gevent.queue.JoinableQueue()
		self._children = gevent.pool.Group()
		self._shutdown = gevent.event.Event()
		self._crashed = gevent.event.Event()
		self._loop = gevent.spawn(self._handle)
		self._loop.link_exception(self._handle_crash)
		self._poisoned_pill = object()
		self.start()

	def _handle(self):
		for task in self._mailbox:
			if isinstance(task, Task):
				try:
					task.set(self.handle(task))
				except Exception as e:
					task.set_exception(e)
					raise
			elif task is self._poisoned_pill:
				self._loop.kill()
			self._mailbox.task_done()

	def _run(self):
		self._shutdown.wait()

	def _handle_crash(self, loop):
		self._crashed.set()

	@property
	def crashed(self):
		return self._crashed.is_set()

	def handle(self, message):
		"""Override in your own Actor subclass"""
		raise NotImplementedError

	def _receive(self, msg):
		sending_greenlet = gevent.getcurrent()
		if isinstance(sending_greenlet, Actor):
			sender = sending_greenlet
		elif (isinstance(sending_greenlet, gevent.greenlet.Greenlet)
		      and isinstance(sending_greenlet.parent, Actor)):
			sender = sending_greenlet.parent
		else:
			sender = None
		task = Task(msg, sender)
		if not self.crashed:
			self._mailbox.put(task)
		else:
			task.set_exception(ActorCrashedError())
		return task

	def tell(self, msg):
		"""Send a message, get nothing (fire-and-forget)."""
		self.receive(msg)

	def ask(self, msg):
		"""Send a message, get a future."""
		return self.receive(msg)

	def await(self, msg):
		"""Send a message, get a result"""
		return self.receive(msg).get()

	def continue(self):
		"""If Actor crashed, continue where it left off,"""
		if not self._loop.dead:
			return
		self._crashed.clear()
		self._loop = gevent.spawn(self._loop)

	def restart(self):
		"""Clear the mailbox, then continue processing."""
		[child.restart() for child in self._children]
		if not self._loop.dead:
			self._loop.kill()
		self._mailbox = gevent.queue.JoinableQueue()
		self._crashed.clear()
		self._loop = gevent.spawn(self._loop)

	def shutdown(self):
		"""Shutdown Actor, unrecoverable"""
		self._shutdown.set()
		for child in self._children:
			child.shutdown()
		self._mailbox.put(self._poisoned_pill)

	def register_child(self, child):
		"""Register an already running Actor as child"""
		self._children.add(child)

	def spawn_child(self, cls, *args, **kwargs):
		"""Start an instance of cls(*args, **kwargs) as child"""
		self._children.start(cls(*args, **kwargs))

	def unregister_child(self, child):
		"""Unregister a running Actor from the list of children"""
		self._children.discard(actor)


class Monitor(Actor):
	def __init__(self, policy='restart', children=None):
		super().__init__()
		self._policy = policy
		[self.register_child(child) for child in children] if children else None

	def _run(self):
		self.join()

	def _handle_child_exit(self, child):
		if self._policy == 'restart':
			child.restart()
		elif self._policy == 'continue':
			child.continue()
		elif self._policy == 'escalate':
			self._loop.kill(ActorCrashedError())
		elif self._policy == 'ignore':
			pass
		elif self._policy == 'deplete':
			if len(self._children.greenlets) <= 1:
				self._loop.kill(ActorCrashedError())

	def spawn_child(self, cls, *args, **kwargs):
		child = cls(*args, **kwargs)
		child.link_exception(self._handle_child_exit)
		self._children.start(child)

	def register_child(self, child):
		child.link_exception(self._handle_child_exit)
		super().register_child(child)

	def unregister_child(self, child):
		child.unlink(self._handle_child_exit)
		self._children.discard(child)

class Router(Monitor):
	def _route(self, msg):
		"""Override in your own Router subclass"""
		raise NotImplementedError

	def receive(self, msg):
		if self._children.greenlets:
			target = self._route(msg)
			return target.receive(msg, sender)

class RandomRouter(Router):
	"""Routes received messages to a random child"""
	def _route(self, msg):
		return random.choice(tuple(self._children.greenlets))

class RoundRobinRouter(Router):
	"""Routes received messages to its childdren in a round-robin fashion"""
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._next = self._round_robin()

	def _round_robin(self):
		while len(self._children.greenlets) >= 1:
			for item in self._children.greenlets:
				yield item
		raise StopIteration

	def _route(self, msg):
		try:
			return next(self._next)
		except StopIteration:
			self._loop.kill(ActorCrashedError())

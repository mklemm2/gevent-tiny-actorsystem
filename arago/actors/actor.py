import gevent, gevent.pool, gevent.event, gevent.greenlet, gevent.queue
import random, pickle

class ActorCrashedError(Exception):
	pass

class Task(gevent.event.AsyncResult):
	def __init__(self, msg, sender=None):
		super().__init__()
		self._msg = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
		self.sender = sender

	@property
	def msg(self):
		return pickle.loads(self._msg)

	def set(self, value=None):
		bytes = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
		return super().set(bytes)

	def get(self, *args, **kwargs):
		bytes = super().get(*args, **kwargs)
		try:
			dat = pickle.loads(bytes)
		except:
			dat = bytes
		return dat

class Loop(gevent.greenlet.Greenlet):
	def __init__(self, actor, loop, exc_handler):
		self._run=loop
		super().__init__()
		self.link_exception(exc_handler)
		self._actor=actor
		self.start()

class Actor(gevent.greenlet.Greenlet):
	def __init__(self, name=None):
		super().__init__()
		if name:
			self.name=name
		self._mailbox = gevent.queue.JoinableQueue()
		self._children = gevent.pool.Group()
		self._shutdown = gevent.event.Event()
		self._crashed = gevent.event.Event()
		self._poisoned_pill = object()
		self._loop = Loop(self, self._handle, self._handle_crash)
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
		if isinstance(getattr(sending_greenlet, '_actor', None), Actor):
			sender = sending_greenlet._actor
		elif isinstance(sending_greenlet, Actor):
			sender = sending_greenlet
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
		self._receive(msg)

	def ask(self, msg):
		"""Send a message, get a future."""
		return self._receive(msg)

	def await(self, msg):
		"""Send a message, get a result"""
		return self._receive(msg).get()

	def resume(self):
		"""If Actor crashed, continue where it left off,"""
		if not self._loop.dead:
			return
		self._crashed.clear()
		self._loop = Loop(self, self._handle, self._handle_crash)

	def restart(self):
		"""Clear the mailbox, then continue processing."""
		[child.restart() for child in self._children]
		if not self._loop.dead:
			self._loop.kill()
		self._mailbox.queue.clear()
		self.resume()

	def shutdown(self):
		"""Shutdown Actor, unrecoverable"""
		self._shutdown.set()
		[child.shutdown() for child in self._children]
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

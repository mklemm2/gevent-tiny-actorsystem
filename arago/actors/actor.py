import gevent, gevent.pool, gevent.event, gevent.greenlet, gevent.queue
import random, pickle, logging

class ActorCrashedError(Exception):
	__str__ = lambda x: "ActorCrashedError"

class ActorHaltedError(Exception):
	__str__ = lambda x: "ActorHaltedError"

class ActorShutdownError(Exception):
    __str__ = lambda x: "ActorShutdownError"

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

	def __str__(self):
		return ("<Task, sender={sender}, message={msg}>"
		).format(sender=self.sender, msg=self.msg)

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
		self._logger = logging.getLogger('root')
		self._mailbox = gevent.queue.JoinableQueue()
		self._children = gevent.pool.Group()
		self._shutdown = gevent.event.Event()
		self._crashed = gevent.event.Event()
		self._poisoned_pill = object()
		self._loop = Loop(self, self._handle, self._handle_crash)
		self._loop_callback = None
		self._loop_links = []
		self.start()

	def _handle(self):
		for task in self._mailbox:
			if isinstance(task, Task):
				self._logger.trace("{me} starts handling {task}".format(me=self, task=task))
				try:
					task.set(self.handle(task))
				except Exception as e:
					task.set_exception(e)
					raise
			elif task is self._poisoned_pill:
				self._logger.debug("{me} received poisoned pill.".format(me=self))
				self._loop.kill()
			self._mailbox.task_done()

	def _run(self):
		self._shutdown.wait()
		self._logger.debug("{me} has terminated.".format(me=self))

	def _handle_crash(self, loop):
		self._logger.error(("Main loop of {me} ended with: {exc}.").format(me=self, exc=loop.exception))
		self._crashed.set()

	def rawlink(self, callback):
		self._logger.debug("Linking {cb} to {me}".format(cb=callback, me=self))
		def callback_proxy(child):
			if isinstance(child, Loop):
				self._logger.debug("Main Loop of {me} triggered callback {cb}, " "proxying ...".format(me=self, cb=callback))
			elif isinstance(child, Actor):
				self._logger.debug("{me} triggered callback {cb}, " "passing through ...".format(me=self, cb=callback))
			callback(self)
		#super().rawlink(callback_proxy)
		self._loop.rawlink(callback_proxy)
		self._loop_links.append(callback_proxy)

	def handle(self, task):
		"""Override in your own Actor subclass"""
		raise NotImplementedError

	def _receive(self, msg, sender=None):
		sending_greenlet = gevent.getcurrent()
		if sender:
			sender = sender
		elif isinstance(getattr(sending_greenlet, '_actor', None), Actor):
			sender = sending_greenlet._actor
		elif isinstance(sending_greenlet, Actor):
			sender = sending_greenlet
		else:
			sender = None
		task = Task(msg, sender)
		if not self._crashed.is_set():
			self._mailbox.put(task)
			self._logger.trace("{me} received {task}".format(me=self, task=task))
		else:
			task.set_exception(ActorCrashedError())
			self._logger.warn("{me} has crashed and rejects {task}".format(me=self, task=task))
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

	def halt(self, exc=ActorHaltedError):
		"""Crash actor so it can be resumed"""
		[child.halt() for child in list(self._children)]
		if not self._loop.dead:
			self._loop.kill(exception=exc)
		self._logger.debug("{me} is halting with: {exc}.".format(me=self, exc=exc))

	def resume(self):
		"""If Actor crashed, continue where it left off,"""
		if self._shutdown.is_set():
			self._logger.error("{me} failed to resume, already terminated.".format(me=self))
			raise ActorShutdownError
		[child.resume() for child in self._children]
		if not self._loop.dead:
			return
		self._loop = Loop(self, self._handle, self._handle_crash)
		[self._loop.rawlink(cb) for cb in self._loop_links]
		self._crashed.clear()
		self._logger.debug("{me} has resumed operation.".format(me=self))

	def restart(self):
		"""Clear the mailbox, then continue processing."""
		if self._shutdown.is_set():
			self._logger.error("{me} failed to restart, already terminated.".format(me=self))
			raise ActorShutdownError
		[child.restart() for child in self._children]
		if not self._loop.dead:
			self._loop.kill()
		self._mailbox.queue.clear()
		self.resume()
		self._logger.debug("{me} was successfully restarted.".format(me=self))

	def shutdown(self):
		"""Shutdown Actor, unrecoverable"""
		self._shutdown.set()
		[child.shutdown() for child in list(self._children)]
		self._mailbox.put(self._poisoned_pill)
		self._logger.debug("{me} received order to terminate.".format(me=self))

	def register_child(self, child):
		"""Register an already running Actor as child"""
		self._children.add(child)
		self._logger.debug("{ch} registered as child of {me}.".format(ch=child, me=self))

	def spawn_child(self, cls, *args, **kwargs):
		"""Start an instance of cls(*args, **kwargs) as child"""
		child = cls(*args, **kwargs)
		self.logger.debug("{me} spawned new child {ch}".format(me=self, ch=child))
		self._children.start(child)

	def unregister_child(self, child):
		"""Unregister a running Actor from the list of children"""
		self._children.discard(actor)
		self._logger.debug("{ch} unregistered as child of {me}.".format(ch=child, me=self))

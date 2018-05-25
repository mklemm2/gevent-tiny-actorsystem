import gevent, gevent.event, gevent.greenlet, gevent.queue
import random, pickle, logging
from gevent import GreenletExit

class ActorCrashedError(Exception):
	__str__ = lambda x: "ActorCrashedError"

class ActorStoppedError(Exception):
	__str__ = lambda x: "ActorStoppedError"

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
	def __init__(self, name=None, max_idle=None, ttl=None):
		super().__init__()
		self.name=name if name else "actor-{0}".format(self.minimal_ident)
		self._logger = logging.getLogger('root')
		self._mailbox = gevent.queue.JoinableQueue()
		self._shutdown = gevent.event.Event()
		self._stopped = gevent.event.Event()
		self._poisoned_pill = object()
		self._loop = Loop(self, self._handle, self._handle_main_loop_crash)
		self._loop_callback = None
		self._loop_links = []
		self._max_idle = max_idle
		if max_idle:
			self._logger.debug("{me} has a timeout of {max_idle}".format(me=self, max_idle=max_idle))
			self._idle = gevent.spawn(self._countdown, max_idle)
			gevent.idle()
		if ttl:
			self._logger.debug("{me} has a TTL of {ttl}".format(me=self, ttl=ttl))
			self._timeout = gevent.spawn(self._countdown, ttl)
			gevent.idle()
		self.start()

	def _countdown(self, seconds):
		while True:
			try:
				self._logger.trace("{me} starts counting down from {seconds}".format(me=self, seconds=seconds))
				gevent.sleep(seconds)
				self._logger.trace("{me} has reached timeout of {seconds}, terminating ...".format(me=self, seconds=seconds))
				self.shutdown()
				break
			except GreenletExit:
				if not self._shutdown.is_set():
					self._logger.trace("{me} canceled current timeout.".format(me=self))
					continue
				else:
					self._logger.trace("{me} wanted to cancel current timeout but was too late.".format(me=self))
					break

	def __str__(self):
		return "<{type} \"{name}\">".format(type=type(self).__name__, name=self.name)

	def _handle(self):
		for task in self._mailbox:
			if hasattr(self, '_idle'):
				self._idle.kill(block=False)
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
		self._mailbox.join()
		self._logger.debug("{me} has terminated.".format(me=self))

	def _handle_main_loop_crash(self, loop):
		self._logger.error(("Main loop of {me} ended with: {exc}.").format(me=self, exc=loop.exception))
		self._stopped.set()

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
		if not self._stopped.is_set():
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

	def stop(self, exc=ActorStoppedError):
		"""Crash actor so it can be resumed"""
		if not self._loop.dead:
			self._loop.kill(exception=exc)
		self._logger.debug("{me} is stopping with: {exc}.".format(me=self, exc=exc))

	def resume(self):
		"""If Actor crashed, continue where it left off,"""
		if self._shutdown.is_set():
			self._logger.error("{me} failed to resume, already terminated.".format(me=self))
			raise ActorShutdownError
		if not self._loop.dead:
			self._logger.debug("{me} is not dead, no need to resume.".format(me=self))
			return
		self._loop = Loop(self, self._handle, self._handle_main_loop_crash)
		[self._loop.link(cb) for cb in self._loop_links]
		self._stopped.clear()
		self._logger.debug("{me} has resumed operation.".format(me=self))

	def restart(self):
		"""Clear the mailbox, then continue processing."""
		if self._shutdown.is_set():
			self._logger.error("{me} failed to restart, already terminated.".format(me=self))
			raise ActorShutdownError
		if not self._loop.dead:
			self._loop.kill()
		self._mailbox.queue.clear()
		self.resume()
		self._logger.debug("{me} was successfully restarted.".format(me=self))

	def shutdown(self):
		"""Shutdown Actor, unrecoverable"""
		self._shutdown.set()
		self._mailbox.put(self._poisoned_pill)
		self._logger.debug("{me} received order to terminate.".format(me=self))

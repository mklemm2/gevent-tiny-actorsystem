import gevent, gevent.event, gevent.greenlet, gevent.queue, greenlet
import random, pickle, logging, weakref
from gevent import GreenletExit

class ActorCrashedError(Exception):
	__str__ = lambda x: "ActorCrashedError"

class ActorStoppedError(Exception):
	__str__ = lambda x: "ActorStoppedError"

class ActorShutdownError(Exception):
    __str__ = lambda x: "ActorShutdownError"

class ActorRestartError(Exception):
    __str__ = lambda x: "ActorRestartError"

class ActorMaxIdleError(Exception):
    __str__ = lambda x: "ActorMaxIdleError"

class ActorTTLError(Exception):
    __str__ = lambda x: "ActorTTLError"

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

class Actor(object):
	def __init__(self, name=None, max_idle=None, ttl=None):
		self.name=name if name else "actor-{0}".format(self.minimal_ident)
		self._logger = logging.getLogger('root')
		self._mailbox = gevent.queue.Queue()
		self._stopped = gevent.event.Event()
		self._poisoned_pill = object()
		self._max_idle = max_idle
		self._ttl = ttl
		self._loop = gevent.spawn(self._dequeue, weakref.proxy(self))

	def __str__(self):
		return "<{type} \"{name}\">".format(type=type(self).__name__, name=self.name)

	def _handle(self, task):
		try:
			self._logger.trace("{me} starts handling {task}".format(me=self, task=task))
			task.set(self.handle(task))
			return task
		except Exception as e:
			task.set_exception(e)
			raise

	def _dequeue(self, parent):
		ttl_timeout = gevent.Timeout.start_new(timeout=self._ttl, exception=ActorTTLError)
		max_idle_timeout = gevent.Timeout.start_new(timeout=self._max_idle, exception=ActorMaxIdleError)
		self._stopped.clear()
		try:
			for task in self._mailbox:
				max_idle_timeout.cancel()
				max_idle_timeout.start()
				if self._max_idle:
					self._logger.trace("{me} has canceled timeout of {max_idle} seconds".format(me=self, max_idle=self._max_idle))
				if isinstance(task, Task):
					self._logger.trace("{me} took {task} from mailbox".format(me=self, task=task))
					self._handle(task)
				elif task is self._poisoned_pill:
					self._logger.debug("{me} received poisoned pill.".format(me=self))
					self._loop.kill()
		except ActorMaxIdleError:
			self._logger.trace("{me} has reached max_idle timeout of {sec} seconds.".format(me=self, sec=self._max_idle))
			self.shutdown()
		except ActorTTLError:
			self._logger.trace("{me} has reached ttl timeout of {sec} seconds.".format(me=self, sec=self._ttl))
			self.shutdown()
		except Exception as e:
			self._logger.error(("Main loop of {me} ended with: {exc}.").format(me=self, exc=e))
			#raise
		finally:
			self._stopped.set()
			ttl_timeout.close()
			max_idle_timeout.close()
			gevent.idle()
			self._parent._handle_child_exit(self)

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
		return self._enqueue(task)

	def _enqueue(self, task):
		if self._stopped.is_set():
			self._logger.warn("{me} is stopped and rejects {task}".format(me=self, task=task))
			raise ActorStoppedError
		else:
			self._mailbox.put(task)
			self._logger.trace("{me} received {task}".format(me=self, task=task))
		return task

	def tell(self, msg, sender=None):
		"""Send a message, get nothing (fire-and-forget)."""
		self._receive(msg, sender=sender)

	def ask(self, msg, sender=None):
		"""Send a message, get a future."""
		return self._receive(msg, sender=sender)

	def await(self, msg, sender=None, timeout=None, retry=1):
		"""Send a message, get a result"""
		for it in range(retry):
			try:
				return self._receive(msg, sender=sender).get(timeout=timeout)
			except (ActorShutdownError, ActorStoppedError, gevent.Timeout) as exc:
				continue
		else:
			raise exc

	def stop(self, exc=ActorStoppedError):
		"""Crash actor so it can be resumed"""
		if not self._stopped.is_set():
			self._stopped.set()
			self._loop.kill(exception=exc)
		self._logger.debug("{me} is stopping with: {exc}.".format(me=self, exc=exc))

	def resume(self):
		"""If Actor crashed, continue where it left off,"""
		if self._stopped.is_set():
			self._loop = gevent.spawn(self._dequeue, weakref.proxy(self))
			self._stopped.clear()
			self._logger.debug("{me} has resumed operation.".format(me=self))
		else:
			self._logger.debug("{me} is not dead, no need to resume.".format(me=self))

	def restart(self):
		"""Clear the mailbox, then continue processing."""
		self.stop()
		self.clear_mailbox(exc=ActorRestartError)
		self.resume()
		self._logger.debug("{me} was successfully restarted.".format(me=self))

	def clear_mailbox(self, exc=ActorShutdownError):
		if len(self._mailbox) > 0:
			self._logger.debug("{me} still has {size} task(s) in its mailbox.".format(me=self, size=len(self._mailbox)))
			while len(self._mailbox) > 0:
				task = self._mailbox.get()
				if isinstance(task, Task):
					self._logger.trace("{me} is rejecting {task}: {exc}".format(me=self, task=task, exc=exc))
					task.set_exception(exc)

	def shutdown(self):
		"""Shutdown Actor, unrecoverable"""
		self._stopped.set()
		self.clear_mailbox()
		self._mailbox.put(self._poisoned_pill)
		self._logger.debug("{me} received order to terminate.".format(me=self))

	def __del__(self):
		if not self._stopped.is_set():
			self._stopped.set()
		self.clear_mailbox()
		try:
			self._loop.kill()
		except greenlet.GreenletExit:
			pass
		self._logger.debug("{me} was destroyed properly".format(me=self))

	def register_parent(self, parent):
		self._parent = weakref.proxy(parent)
		self._logger.debug("{me} registered {par} as its parent.".format(me=self, par=parent))

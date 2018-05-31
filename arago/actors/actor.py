import gevent, gevent.event, gevent.greenlet, gevent.queue, greenlet
import random, pickle, logging, weakref
from gevent import GreenletExit

class ActorStoppedError(Exception):
	__str__ = lambda x: "ActorStoppedError"

class ActorMaxIdleError(Exception):
    __str__ = lambda x: "ActorMaxIdleError"

class ActorTTLError(Exception):
    __str__ = lambda x: "ActorTTLError"

class TaskCanceledError(Exception):
    __str__ = lambda x: "TaskCanceledError"

class Task(gevent.event.AsyncResult):
	def __init__(self, msg, sender=None):
		super().__init__()
		self._msg = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
		self.sender = sender
		self.canceled = False

	def cancel(self):
		self.canceled = True

	@property
	def msg(self):
		return pickle.loads(self._msg)

	def set(self, value=None):
		if self.canceled:
			return
		bytes = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
		return super().set(bytes)

	def get(self, *args, **kwargs):
		if self.canceled:
			return TaskCanceledError
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
		self._stopped = False
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
		self._stopped = False
		try:
			for task in self._mailbox:
				max_idle_timeout.cancel()
				max_idle_timeout.start()
				if self._max_idle:
					self._logger.trace("{me} has canceled timeout of {max_idle} seconds".format(me=self, max_idle=self._max_idle))
				if isinstance(task, Task) and not task.canceled:
					self._logger.trace("{me} took {task} from mailbox".format(me=self, task=task))
					self._handle(task)
				elif task is self._poisoned_pill:
					self._logger.debug("{me} received poisoned pill.".format(me=self))
					raise ActorStoppedError
				elif isinstance(task, Task) and task.canceled:
					self._logger.trace("{me} took canceled {task} from mailbox, dismissing".format(me=self, task=task))
					continue
		except ActorMaxIdleError as e:
			self._logger.trace("{me} has reached max_idle timeout of {sec} seconds.".format(me=self, sec=self._max_idle))
			self._kill()
		except ActorTTLError as e:
			self._logger.trace("{me} has reached ttl timeout of {sec} seconds.".format(me=self, sec=self._ttl))
			self._kill()
		except ActorStoppedError as e:
			self._stopped = True
			self._parent._handle_child(self, "stopped")
		except Exception as e:
			self._stopped = True
			self._logger.error(("{me} crashed with: {exc}").format(me=self, exc=e))
			self._parent._handle_child(self, "crashed")
		finally:
			ttl_timeout.close()
			max_idle_timeout.close()

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
		if self._stopped:
			self._logger.warn("{me} is stopped and rejects {task}".format(me=self, task=task))
			raise ActorStoppedError
		else:
			self._mailbox.put(task)
			self._logger.trace("{me} received {task}".format(me=self, task=task))
		return task

	def _kill(self):
		self._stopped = True
		self._logger.debug("{me} received order to stop immediately.".format(me=self))
		self._parent._handle_child(self, "stopped")
		try:
			self._loop.kill()
		except GreenletExit:
			pass
		self.clear()

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
			except (ActorStoppedError, gevent.Timeout) as exc:
				last_exc = exc
				continue
		raise last_exc

	def start(self):
		if self._stopped:
			self._loop = gevent.spawn(self._dequeue, weakref.proxy(self))
			self._stopped = False
			self._logger.debug("{me} has resumed operation.".format(me=self))
		else:
			self._logger.debug("{me} is already started.".format(me=self))

	def stop(self):
		if not self._stopped:
			self._stopped = True
			self._logger.debug("{me} received order to stop.".format(me=self))
			self._mailbox.put(self._poisoned_pill)
		else:
			self._logger.debug("{me} is already stopped.".format(me=self))

	def clear(self):
		while len(self._mailbox) > 0:
			task = self._mailbox.get()
			if isinstance(task, Task):
				self._logger.trace("{me} is rejecting {task}".format(me=self, task=task))
				task.set_exception(ActorStoppedError)

	def __del__(self):
		try:
			self._loop.kill()
		except GreenletExit:
			pass
		self.clear()
		self._logger.debug("{me} was destroyed properly".format(me=self))

	def register_parent(self, parent):
		self._parent = weakref.proxy(parent)
		self._logger.debug("{me} registered {par} as its parent.".format(me=self, par=parent))

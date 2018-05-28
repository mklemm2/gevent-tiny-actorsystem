from arago.actors.actor import Actor
from arago.actors.actor import ActorCrashedError, ActorStoppedError, ActorShutdownError

class ExitPolicy(object):
	def __init__(self, identifier):
		self.__ident__ = identifier
	def __str__(self):
		return self.__ident__

SHUTDOWN = ExitPolicy("SHUTDOWN") # shutdown crashed children
IGNORE = ExitPolicy("IGNORE") # ignore child crashes
RESUME = ExitPolicy("RESUME") # resume the exited child
RESTART = ExitPolicy("RESTART") # restart the exited child
RESTART_REST = ExitPolicy("RESTART_REST") # restart the exited child and all that came after it (in order)
RESTART_REST_REVERSE = ExitPolicy("RESTART_REST_REVERSE") # restart the exited child and all that came after it (in reverse order)
RESTART_ALL = ExitPolicy("RESTART_ALL") # restart all children (in order)
RESTART_ALL_REVERSE = ExitPolicy("RESTART_ALL_REVERSE") # restart all children (in reverse order)
ESCALATE = ExitPolicy("ESCALATE") # if a child stops, stop all children and yourself
DEPLETE = ExitPolicy("DEPLETE") # if the last child stops, stop all children and yourself

class Monitor(Actor):
	def __init__(self, name=None, policy=RESTART, max_restarts=None, timeframe=None, children=None):
		super().__init__(name=name)
		self._policy = policy
		self._children = []
		([self.register_child(child) for child in children]
		 if children else None)

	def _handle_child_main_loop_exit(self, child_loop):
		child = child_loop._actor
		return self._handle_child_exit(child)

	def _handle_child_exit(self, child):
		#actor = child._actor
		try:
			raise child._loop.exception or ActorShutdownError
		except ActorStoppedError as e:
			return self._handle_child_stop(child, e)
		except ActorShutdownError as e:
			return self._handle_child_termination(child, e)
		except (ActorCrashedError, Exception) as e:
			return self._handle_child_crash(child, e)

	def _handle_child_crash(self, child, exc):
		self._logger.debug("{ch}, a child of {me}, crashed with: {exc}".format(ch=child, me=self, exc=exc))
		return self._handle_child(child)

	def _handle_child_stop(self, child, exc):
		self._logger.debug("{ch}, a child of {me}, stopped with: {exc}".format(ch=child, me=self, exc=child.exc))
		return self._handle_child(child)

	def _handle_child_termination(self, child, exc):
		self._logger.debug("{ch}, a child of {me}, terminated with: {exc}".format(ch=child, me=self, exc=exc))
		return self._handle_child(child)

	def _handle_child(self, child):
		if self._stopped.is_set():
			self._logger.debug("Monitor is stopped!")
			return
		else:
			self._logger.debug("{ch}, a child of {me}, stopped, policy is {pol}".format(ch=child, me=self, pol=self._policy))
		if self._policy == RESTART:
			try:
				child.restart()
			except ActorShutdownError:
				self._logger.error("{me} failed to restart {ch}, escalating ...".format(me=self, ch=child))
				self.unregister_child(child)
				self.stop(ActorCrashedError)

		elif self._policy == RESUME:
			try:
				child.resume()
			except ActorShutdownError:
				self._logger.error("{me} failed to resume {ch}, escalating ...".format(me=self, ch=child))
				self.unregister_child(child)
				self.stop(ActorCrashedError)

		elif self._policy == SHUTDOWN:
			self._logger.warn("{ch}, a child of {me}, stopped, shutting it down ...".format(me=self, ch=child))
			child.shutdown()
			self.unregister_child(child)

		elif self._policy == ESCALATE:
			self._loop.kill(ActorCrashedError)
			self._logger.error("{ch}, a child of {me}, stopped, escalating ...".format(me=self, ch=child))

		elif self._policy == IGNORE:
			self._logger.warn("{ch}, a child of {me}, stopped, ignoring ...".format(me=self, ch=child))
			self.unregister_child(child)

		elif self._policy == DEPLETE:
			self.unregister_child(child)
			if len(self._children.greenlets) <= 1:
				self._loop.kill(ActorCrashedError)
				self._logger.error("{ch}, last child of {me}, stopped, escalating ...".format(me=self, ch=child))

	def spawn_child(self, cls, *args, **kwargs):
		"""Start an instance of cls(*args, **kwargs) as child"""
		child = cls(*args, **kwargs)
		self.logger.debug("{me} spawned new child {ch}".format(me=self, ch=child))
		self._register_child(child)

	def register_child(self, child):
		"""Register an already running Actor as child"""
		self._children.append(child)
		#child.link(self._handle_child_exit)
		child.register_parent(self)
		self._logger.debug("{ch} registered as child of {me}.".format(ch=child, me=self))

	def unregister_child(self, child):
		"""Unregister a running Actor from the list of children"""
		#child.unlink(self._handle_child_exit)
		self._children.remove(child)
		self._logger.debug("{ch} unregistered as child of {me}.".format(ch=child, me=self))

	def resume(self):
		[child.resume() for child in self._children]
		super().resume()

	def restart(self):
		[child.restart() for child in self._children]
		super().restart()

	def shutdown(self):
		[child.shutdown() for child in self._children]
		super().shutdown()

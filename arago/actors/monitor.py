from arago.actors.actor import Actor, ActorShutdownError, ActorCrashedError

class ExitPolicy(object):
	def __init__(self, identifier):
		self.__ident__ = identifier
	def __str__(self):
		return self.__ident__

RESTART = ExitPolicy("RESTART")
RESUME = ExitPolicy("RESUME")
ESCALATE = ExitPolicy("ESCALATE")
IGNORE = ExitPolicy("IGNORE")
DEPLETE = ExitPolicy("DEPLETE")

class Monitor(Actor):
	def __init__(self, policy=RESTART, children=None):
		super().__init__()
		self._policy = policy
		([self.register_child(child) for child in children]
		 if children else None)

	def _handle_child_exit(self, child):
		self._logger.debug("{ch}, a child of {me} exited, policy is {pol}".format(ch=child, me=self, pol=self._policy))
		if self._policy == RESTART:
			try:
				child.restart()
			except ActorShutdownError:
				self._logger.error("{me} failed to restart {ch}, escalating ...".format(me=self, ch=child))
				self.unregister_child(child)
				self.halt(ActorCrashedError)
		elif self._policy == RESUME:
			try:
				child.resume()
			except ActorShutdownError:
				self._logger.error("{me} failed to resume {ch}, escalating ...".format(me=self, ch=child))
				self.unregister_child(child)
				self.halt(ActorCrashedError)
		elif self._policy == ESCALATE:
			self._loop.kill(ActorCrashedError)
			self._logger.error("{ch}, a child of {me} crashed, escalating ...".format(me=self, ch=child))
		elif self._policy == IGNORE:
			self.unregister_child(child)
			self._logger.warn("{ch}, a child of {me} crashed, ignoring ...".format(me=self, ch=child))
		elif self._policy == DEPLETE:
			self.unregister_child(child)
			if len(self._children.greenlets) <= 1:
				self._loop.kill(ActorCrashedError)
				self._logger.error("{ch}, last child of {me} crashed, escalating ...".format(me=self, ch=child))

	def spawn_child(self, cls, *args, **kwargs):
		child = cls(*args, **kwargs)
		child.link(self._handle_child_exit)
		self._children.start(child)

	def register_child(self, child):
		child.link(self._handle_child_exit)
		super().register_child(child)

	def unregister_child(self, child):
		child.unlink(self._handle_child_exit)
		self._children.discard(child)

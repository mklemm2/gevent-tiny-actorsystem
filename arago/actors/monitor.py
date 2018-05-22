from arago.actors.actor import Actor

class Monitor(Actor):
	def __init__(self, policy='restart', children=None):
		super().__init__()
		self._policy = policy
		([self.register_child(child) for child in children]
		 if children else None)

	def _run(self):
		self.join()

	def _handle_child_exit(self, child):
		if self._policy == 'restart':
			child.restart()
		elif self._policy == 'continue':
			child.resume()
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

from arago.actors import Router, ActorCrashedError

class RoundRobinRouter(Router):
	("""Routes received messages to its childdren """
	 """in a round-robin fashion""")
	def __init__(self, name=None, *args, **kwargs):
		super().__init__(name=name, *args, **kwargs)
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
			self._loop.kill(ActorCrashedError)

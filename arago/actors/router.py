from arago.actors.monitor import Monitor
from collections import Counter
from uhashring import HashRing

class Router(Monitor):
	def _route(self, msg):
		"""Override in your own Router subclass"""
		raise NotImplementedError

	def _receive(self, msg):
		target = self._route(msg)
		return target._receive(msg)

class RandomRouter(Router):
	"""Routes received messages to a random child"""
	def _route(self, msg):
		return random.choice(tuple(self._children.greenlets))

class RoundRobinRouter(Router):
	("""Routes received messages to its childdren """
	 """in a round-robin fashion""")
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

class ConsistentHashingRouter(Router):
	("""Applies mapfunc to each message and based on the result, """
	 """routes equal messages to always the same child""")
	def __init__(self, mapfunc=None, *args, **kwargs):
		self._hashring = HashRing()
		self._map = mapfunc if callable(mapfunc) else lambda msg: msg
		super().__init__(*args, **kwargs)

	def _route(self, msg):
		return self._hashring.get_node(self._map(msg))

	def register_child(self, child):
		super().register_child(child)
		self._hashring.add_node(child)

	def unregister_child(self, child):
		super().unregister_child(child)
		self._hashring.remove_node(child)

class BroadcastRouter (Router):
	"""Routes received messages to all children"""
	def _receive(self, msg):
		return (target._receive(msg) for target
		        in self._children.greenlets)

	def await(self, msg):
		"""Wait for all AsyncResults to become ready"""
		return Counter((result.get() for result in self._receive(msg)))

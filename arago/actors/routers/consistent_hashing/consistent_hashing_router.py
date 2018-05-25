from arago.actors import Router
from uhashring import HashRing

class ConsistentHashingRouter(Router):
	("""Applies mapfunc to each message and based on the result, """
	 """routes equal messages to always the same child""")
	def __init__(self, name=None, mapfunc=None, *args, **kwargs):
		self._hashring = HashRing()
		self._map = mapfunc if callable(mapfunc) else lambda msg: msg
		super().__init__(name=name, *args, **kwargs)

	def _route(self, msg):
		return self._hashring.get_node(self._map(msg))

	def register_child(self, child):
		super().register_child(child)
		self._hashring.add_node(child)

	def unregister_child(self, child):
		super().unregister_child(child)
		self._hashring.remove_node(child)

from arago.actors import Router

class BroadcastRouter (Router):
	"""Routes received messages to all children"""
	def _receive(self, msg):
		return (target._receive(msg) for target
		        in self._children.greenlets)

	def await(self, msg):
		"""Wait for all AsyncResults to become ready"""
		return [result.get() for result in self._receive(msg)]

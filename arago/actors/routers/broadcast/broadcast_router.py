from arago.actors import Router

class BroadcastRouter (Router):
	"""Routes received messages to all children"""
	def _forward(self, task):
		for target in self._children:
			target._enqueue(task)

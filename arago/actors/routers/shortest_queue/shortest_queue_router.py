from arago.actors import Router

class ShortestQueueRouter(Router):
	"""Routes received messages to the child with the lowest number of enqueued tasks"""
	def _route(self, msg):
		return sorted(self._children, key=lambda item: len(item._mailbox), reverse=False)[0]

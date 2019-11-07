from arago.actors import Router

class ShortestQueueRouter(Router):
	"""Routes received messages to the child with the lowest number of enqueued tasks"""
	def _route(self, msg):
		# Try to simply find a free worker, first
		for item in self._children:
			if hasattr(item, "_busy") and not item._busy:
				return item
		# If all workers are busy, order them by queue length and return the first
		# FIXME: Can probably be optimized by not sorting the whole list
		item = sorted(self._children, key=lambda item: len(item._mailbox), reverse=False)[0]
		return item

from arago.actors import Router
import random

class RandomRouter(Router):
	"""Routes received messages to a random child"""
	def _route(self, msg):
		return random.choice(tuple(self._children.greenlets))

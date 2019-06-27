from arago.actors import Router

class MappingRouter(Router):
	"""Mapping-table based routing"""
	def __init__(self, mapfunc, maptable, name=None, *args, **kwargs):
		super().__init__(name=name, *args, **kwargs)
		self.mapfunc = mapfunc
		self.maptable = maptable

	def _route(self, msg):
		try:
			self._logger.debug(f"{self} is routing message {msg}")
			signature = self.mapfunc(msg)
			return self.maptable[signature]
		except KeyError:
			self._logger.warning(f"{self} could not forward {msg}, no entry in mapping table for {signature}.")

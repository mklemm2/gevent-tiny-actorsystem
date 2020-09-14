from arago.actors import Router, IGNORE
import gevent.lock
import gc
import better_exceptions
import sys


class SpawningChildFailedError(Exception):
	pass


class OnDemandRouter(Router):
	"""Spawns new children on demand"""
	def __init__(self, worker_cls, name=None, worker_name_tpl=None, worker_args_func=None,
	             policy=IGNORE, max_restarts=None, timeframe=None,
	             mapfunc=None):
		self._worker_cls = worker_cls
		self._worker_name_tpl = worker_name_tpl
		self._worker_args_func = worker_args_func
		self._map = mapfunc if callable(mapfunc) else lambda msg: id(msg)
		self._children_map = {}
		self._children_map_reverse = {}
		self._children_map_lock = gevent.lock.Semaphore()
		super().__init__(name=name, policy=policy, max_restarts=max_restarts, timeframe=timeframe, children=None)

	def _route(self, msg):
		target = self._map(msg)
		if target in self._children_map:
			child = self._children_map[target]
			self._logger.debug("{me} is re-using existing worker {ch} for target {target}".format(me=self, ch=child, target=target))
		else:
			child = self.spawn_child(target, msg=msg)
			self._logger.verbose("{me} has spawned new worker {ch} for target {target}".format(me=self, ch=child, target=target))
		return child

	def spawn_child(self, target, name=None, msg=None):
		if not name:
			name = "{tpl}-{target}".format(tpl = self._worker_name_tpl or str(self._worker_cls), target=target)
		try:
			kwargs = self._worker_args_func(msg) if msg and self._worker_args_func else {}
			child = self._worker_cls(name=name, target=target, **kwargs)
		except Exception as err:
			formatted_exc = better_exceptions.format_exception(*sys.exc_info())
			self._logger.error("Spawning child failed with {e}".format(e=formatted_exc))
			raise SpawningChildFailedError(err)
		child._target = target
		self.register_child(child, target)
		return child

	def register_child(self, child, target):
		super().register_child(child)
		with self._children_map_lock:
			self._children_map[target] = child

	def unregister_child(self, child):
		with self._children_map_lock:
			target = child._target
			try:
				super().unregister_child(child)
				del self._children_map[target]
				gc.collect()
			except KeyError:
				self._logger.debug("{me} failed to unregister {ch}: Not registered (any more?)".format(me=self, ch=child))

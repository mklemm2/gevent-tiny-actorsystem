from arago.actors import Router, IGNORE

class OnDemandRouter(Router):
	"""Spawns new children on demand"""
	def __init__(self, worker_cls, name=None, worker_name_tpl=None, policy=IGNORE,
	             mapfunc=None, children=None,
	             *worker_args, **worker_kwargs):
		self._worker_cls = worker_cls
		self._worker_args = worker_args
		self._worker_kwargs = worker_kwargs
		self._worker_name_tpl = worker_name_tpl
		self._map = mapfunc if callable(mapfunc) else lambda msg: msg
		self._children_map = {}
		self._children_map_reverse = {}
		super().__init__(name=name, policy=policy, children=None)

	def _route(self, msg):
		target = self._map(msg)
		if target in self._children_map:
			child = self._children_map[target]
			self._logger.debug("{me} is re-using existing worker {ch} for target {target}".format(me=self, ch=child, target=target))
		else:
			name_tpl = self._worker_name_tpl or str(self._worker_cls)
			child = self.spawn_child(
				self._worker_cls, target,
				name="{tpl}-{target}".format(
					tpl=name_tpl, target=target),
				*self._worker_args, **self._worker_kwargs)
			self._logger.verbose("{me} has spawned new worker {ch} for target {target}".format(me=self, ch=child, target=target))
		return child

	def spawn_child(self, cls, target, *args, **kwargs):
		child = cls(*args, **kwargs)
		self.register_child(child, target)
		return child

	def register_child(self, child, target):
		super().register_child(child)
		self._children_map[target] = child
		self._children_map_reverse[child] = target

	def unregister_child(self, child, target=None):
		if target and target in self._children_map:
			child = self._children_map[target]
		elif child in self._children_map_reverse:
			target = self._children_map_reverse[child]
		else:
			self._logger.warn("Cannot unregister child {ch} from {me}: Not registered!".format(ch=child, me=self))
			return
		del self._children_map[target]
		del self._children_map_reverse[child]
		super().unregister_child(child)

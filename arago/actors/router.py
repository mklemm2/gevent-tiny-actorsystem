from arago.actors.monitor import Monitor, IGNORE
from arago.actors.actor import Task, Actor, ActorCrashedError
from collections import Counter
from uhashring import HashRing
import weakref

class Router(Monitor):
	def _route(self, msg):
		"""Override in your own Router subclass"""
		raise NotImplementedError

	def _receive(self, msg):
		if not self._crashed.is_set():
			target = self._route(msg)
			task = target._receive(msg)
		else:
			task = Task(msg)
			task.set_exception(ActorCrashedError)
			self._logger.warn("{me} has crashed and rejects {task}".format(me=self, task=task))
		return task

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

class OnDemandRouter(Router):
	"""Spawns new children on demand"""
	def __init__(self, worker_cls, worker_name_tpl=None, policy=IGNORE,
	             mapfunc=None, children=None,
	             *worker_args, **worker_kwargs):
		self._worker_cls = worker_cls
		self._worker_args = worker_args
		self._worker_kwargs = worker_kwargs
		self._worker_name_tpl = worker_name_tpl
		self._map = mapfunc if callable(mapfunc) else lambda msg: msg
		self._children_map = {}
		self._children_map_reverse = {}
		super().__init__(policy=policy, children=None)

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


class BroadcastRouter (Router):
	"""Routes received messages to all children"""
	def _receive(self, msg):
		return (target._receive(msg) for target
		        in self._children.greenlets)

	def await(self, msg):
		"""Wait for all AsyncResults to become ready"""
		return Counter((result.get() for result in self._receive(msg)))

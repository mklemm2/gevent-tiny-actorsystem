from arago.actors.source import Source
import time
import gevent

class Timer(object):
	def __init__(self, handler, delay, timeout):
		self.handler = handler
		self.timer = gevent.get_hub().loop.timer(after=delay, repeat=timeout)

	def start(self):
		self.timer.start(self.handler)

	def stop(self):
		self.timer.stop()

	def close(self):
		self.timer.close()


class TimerSource(Source):
	def __init__(self, handler=None, delay=0, timeout=1, msg="wakeup", *args, **kwargs):
		self._handler = handler
		self._msg = msg
		self._timer = Timer(self._wakeup, delay, timeout)
		super().__init__(*args, **kwargs, server=self._timer)

	def stop(self):
		super().stop()
		self._timer.close()

	def _wakeup(self):
		now = time.time()
		self._logger.debug("{me} triggered at {ts}".format(me=self, ts=now))
		if self._handler:
			gevent.spawn(self._handler.tell, self._msg, {"timestamp": now}, self)
		else:
			self._logger.warning("{me} has no handler defined!")

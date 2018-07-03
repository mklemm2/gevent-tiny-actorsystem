#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
from arago.actors import Actor, Root
from arago.common.logging import getCustomLogger
import function_pattern_matching as fpm
import sys, gevent

def match(*args, **kwargs):
        return fpm.case(fpm.guard(*args, **kwargs))

logger = getCustomLogger(
	level="TRACE", logfile=sys.stderr,
	formatting=("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
)

class PingPong(Actor):
	def aufschlag(self, opponent):
		opponent.tell("Ping")
	@match
	def handle(self, msg: fpm.eq("Ping"), sender):
		#gevent.sleep(2)
		sender.tell("Pong")
	@match
	def handle(self, msg: fpm.eq("Pong"), sender):
		#gevent.sleep(2)
		sender.tell("Ping")

players = [PingPong(name="Player One"), PingPong(name="Player Two")]

players[0].aufschlag(players[1])
Root(name="root", children=players)

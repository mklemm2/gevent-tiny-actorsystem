#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()
import requests, json, gevent, random, gevent.pool

targets = ["node-{0}".format(item) for item in range(10)]
greetings = ["hello", "hallo", "gruezdi", "aloa", "moin", "ahoi", "servus", "crash", "die"]

session = requests.Session()

def rest_call(i):#
	greeting = random.choice(greetings)
	target = random.choice(targets)
	r = session.post("http://localhost:8080", json={"target": target, "payload": greeting})
	print("{i:3} Sending {gr} to {tg}: {an}".format(i=i, gr=greeting, tg=target, an=r.text))

p = gevent.pool.Pool(size=10)

g = [p.spawn(rest_call, i) for i in range(1000)]

gevent.joinall(g)

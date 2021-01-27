#!/usr/bin/env python
import os
if os.environ.get('USER','') == 'vagrant':
	del os.link

import distutils.core

distutils.core.setup(
	name = "python-arago-actors",
	version = "1.0",
	author = "Marcus Klemm",
	author_email = "mklemm@arago.de",
	description = ("gevent-based actor system"),
	license = "MIT",
	url = "http://www.arago.de",
	long_description="gevent-based actor system",
	classifiers=[
		"Development Status :: 5 - Production/Stable",
		"Topic :: Utilities",
		"License :: OSI Approved :: MIT License",
	],
	packages=['arago.actors',
			  'arago.actors.routers.broadcast',
			  'arago.actors.routers.consistent_hashing',
			  'arago.actors.routers.mapping',
			  'arago.actors.routers.on_demand',
			  'arago.actors.routers.random',
			  'arago.actors.routers.round_robin',
			  'arago.actors.routers.shortest_queue',
			  'arago.actors.sources.timer',
			  'arago.actors.sources.rest'
	],
	install_requires=['gevent', 'function_pattern_matching', 'better_exceptions']
)

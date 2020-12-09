# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the \"License\"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an \"AS IS\" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python

import logging

from argparse import ArgumentParser
from BaseHTTPServer import HTTPServer
from os import chdir
from SimpleHTTPServer import SimpleHTTPRequestHandler
from SocketServer import ThreadingMixIn

# Needs to be threaded or health check hangs the server
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
  pass

if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  parser = ArgumentParser(description='Serve up directory over http')
  parser.add_argument('--dir', default='/var/www')
  parser.add_argument('--port', type=int, default=8080)

  logging.debug('About to parse arguments')
  args = parser.parse_args()

  logging.debug('Serving directory ' + args.dir + ' via HTTP at port ' + str(args.port))

  chdir(args.dir)

  server = ThreadedHTTPServer(('', args.port), SimpleHTTPRequestHandler)
  server.serve_forever()
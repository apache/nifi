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
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from subprocess import PIPE, Popen

# Needs to be threaded or health check hangs the server
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
  pass

class TailHTTPRequestHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.send_header('Content-type','text/plain')
    self.end_headers()
    args = ['tail', '-f', '-n', '+1']
    args.extend(TAIL_FILES)
    p = Popen(args, stdout=PIPE)
    try:
      for line in iter(p.stdout.readline, b''):
        self.wfile.write(line)
        self.wfile.flush()
    finally:
      p.kill()
    return

if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  parser = ArgumentParser(description='Tail file over http')
  parser.add_argument('--file', action='append')
  parser.add_argument('--port', type=int, default=8000)

  logging.debug('About to parse arguments')
  args = parser.parse_args()

  if not args.file:
    raise Exception('Must specify --file')

  global TAIL_FILES
  TAIL_FILES = args.file

  logging.debug('Serving tail of ' + str(TAIL_FILES) + ' via HTTP at port ' + str(args.port))

  server = ThreadedHTTPServer(('', args.port), TailHTTPRequestHandler)
  server.serve_forever()
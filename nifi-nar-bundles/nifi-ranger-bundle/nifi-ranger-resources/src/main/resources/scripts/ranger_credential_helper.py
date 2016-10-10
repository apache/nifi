#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
from subprocess import  Popen,PIPE
from optparse import OptionParser

if os.getenv('JAVA_HOME') is None:
	print "[W] ---------- JAVA_HOME environment property not defined, using java in path. ----------"
	JAVA_BIN='java'
else:
	JAVA_BIN=os.path.join(os.getenv('JAVA_HOME'),'bin','java')
print "Using Java:" + str(JAVA_BIN)

def main():

	parser = OptionParser()

	parser.add_option("-l", "--libpath", dest="library_path", help="Path to folder where credential libs are present")
	parser.add_option("-f", "--file",  dest="jceks_file_path", help="Path to jceks file to use")
	parser.add_option("-k", "--key",  dest="key", help="Key to use")
	parser.add_option("-v", "--value",  dest="value", help="Value to use")
	parser.add_option("-c", "--create",  dest="create", help="Add a new alias")

	(options, args) = parser.parse_args()
	library_path = options.library_path
	jceks_file_path = options.jceks_file_path
	key = options.key
	value = options.value
	getorcreate = 'create' if options.create else 'get'
	call_keystore(library_path, jceks_file_path, key, value, getorcreate)


def call_keystore(libpath, filepath, aliasKey, aliasValue='', getorcreate='get'):
	finalLibPath = libpath.replace('\\','/').replace('//','/')
	finalFilePath = 'jceks://file/'+filepath.replace('\\','/').replace('//','/')
	if getorcreate == 'create':
		commandtorun = [JAVA_BIN, '-cp', finalLibPath, 'org.apache.ranger.credentialapi.buildks' ,'create', aliasKey, '-value', aliasValue, '-provider',finalFilePath]
		p = Popen(commandtorun,stdin=PIPE, stdout=PIPE, stderr=PIPE)
		output, error = p.communicate()
		statuscode = p.returncode
		if statuscode == 0:
			print "Alias " + aliasKey + " created successfully!"
		else :
			print "Error creating Alias!! Error: " + str(error)
		
	elif getorcreate == 'get':
		commandtorun = [JAVA_BIN, '-cp', finalLibPath, 'org.apache.ranger.credentialapi.buildks' ,'get', aliasKey, '-provider',finalFilePath]
		p = Popen(commandtorun,stdin=PIPE, stdout=PIPE, stderr=PIPE)
		output, error = p.communicate()
		statuscode = p.returncode
		if statuscode == 0:
			print "Alias : " + aliasKey + " Value : " + str(output)
		else :
			print "Error getting value!! Error: " + str(error)
		
	else:
		print 'Invalid Arguments!!'
	
if __name__ == '__main__':
	main()

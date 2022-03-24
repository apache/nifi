#! /usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import traceback
from org.apache.nifi.processor.io import InputStreamCallback
from java.io import BufferedReader, InputStreamReader

class ReadFirstLine(InputStreamCallback) :
    __line = None;

    def __init__(self) :
        pass

    def getLine(self) :
        return self.__line

    def process(self, input) :
        try :
            reader = InputStreamReader(input)
            bufferedReader = BufferedReader(reader)
            self.__line = bufferedReader.readLine()
        except :
            print "Exception in Reader:"
            print '-' * 60
            traceback.print_exc(file=sys.stdout)
            print '-' * 60
            raise
        finally :
            if bufferedReader is not None :
                bufferedReader.close()
            if reader is not None :
                reader.close()

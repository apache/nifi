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
from org.python.core.io import TextIOWrapper,BufferedReader,BufferedWriter,StreamIO
from org.apache.nifi.scripting import OutputStreamHandler

class WriteFirstLine(OutputStreamHandler):
    def __init__(self, wrappedIn):
        self.wrappedIn = wrappedIn
    
    def write(self, output):
        streamOut = StreamIO(output, False)
        bufWrtr = BufferedWriter(streamOut, 8192)
        wrappedOut = TextIOWrapper(bufWrtr)
        wrappedOut.write(self.wrappedIn.readline(8192))
        wrappedOut.flush()
        wrappedOut.close()
    
class WriteOtherLines(OutputStreamHandler):
    def __init__(self, wrappedIn):
        self.wrappedIn = wrappedIn
        
    def write(self, output):
        streamOut = StreamIO(output, False)
        bufWrtr = BufferedWriter(streamOut, 8192)
        wrappedOut = TextIOWrapper(bufWrtr)
        line = self.wrappedIn.readline(8192)
        while line != '':
            wrappedOut.write(line)
            line = self.wrappedIn.readline(8192)
        wrappedOut.flush()
        wrappedOut.close()

class SimpleConverter(ConverterScript):

    def convert(self, input):
        streamIn = StreamIO(input, False)
        bufRdr = BufferedReader(streamIn, 8192)
        wrappedIn = TextIOWrapper(bufRdr)
        
        writeFirstLine = WriteFirstLine(wrappedIn)
        self.createFlowFile("firstLine", self.FAIL_RELATIONSHIP, writeFirstLine)

        writeOtherLines = WriteOtherLines(wrappedIn)                
        self.createFlowFile("otherLines", self.SUCCESS_RELATIONSHIP, writeOtherLines)     
        
instance = SimpleConverter()
        
        
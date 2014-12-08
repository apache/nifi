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
import re

class SimpleJythonReader(ReaderScript):
    def getPropertyDescriptors( self ):
        nev = StandardValidators.NON_EMPTY_VALIDATOR   
        return [PropertyDescriptor.Builder().name("expr").required(1).addValidator(nev).build()]
        
    def route( self, input ):
        expr = self.getProperty("expr")
        filename = self.getAttribute("filename")
        self.setAttribute("filename", filename + ".modified")
        for line in FileUtil.wrap(input):
            if re.match(expr, line): return self.FAIL_RELATIONSHIP 

        return self.SUCCESS_RELATIONSHIP

instance = SimpleJythonReader()


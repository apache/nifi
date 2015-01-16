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

class RoutingReader(ReaderScript):
    A = Relationship.Builder().name("a").description("some good stuff").build()
    B = Relationship.Builder().name("b").description("some other stuff").build()
    C = Relationship.Builder().name("c").description("some bad stuff").build()
    
    def getRelationships(self):
        return [self.A,self.B,self.C]
  
    def getExceptionRoute(self):
        return self.C
  
    def route( self, input ):
        for line in FileUtil.wrap(input):
            if re.match("^bad", line, re.IGNORECASE):
                return self.B
            if re.match("^sed", line):
                raise RuntimeError("That's no good!")

        return self.A

instance = RoutingReader()

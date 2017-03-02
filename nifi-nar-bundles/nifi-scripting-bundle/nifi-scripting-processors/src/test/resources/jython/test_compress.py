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
from callbacks import Compress, Decompress
from org.apache.nifi.processor import Processor
from org.apache.nifi.processor import Relationship
from org.apache.nifi.components import PropertyDescriptor

class CompressFlowFile(Processor) :
    __rel_success = Relationship.Builder().description("Success").name("success").build()

    def __init__(self) :
        pass

    def initialize(self, context) :
        pass

    def getRelationships(self) :
        return set([self.__rel_success])

    def validate(self, context) :
        pass

    def getPropertyDescriptors(self) :
        descriptor = PropertyDescriptor.Builder().name("mode").allowableValues("compress", "decompress").required(True).build()
        return [descriptor]

    def onPropertyModified(self, descriptor, newValue, oldValue) :
        pass

    def onTrigger(self, context, sessionFactory) :
        session = sessionFactory.createSession()
        try :
            # ensure there is work to do
            flowfile = session.get()
            if flowfile is None :
                return

            if context.getProperty("mode").getValue() == "compress" :
                flowfile = session.write(flowfile, Compress())
            else :
                flowfile = session.write(flowfile, Decompress())

            # transfer
            session.transfer(flowfile, self.__rel_success)
            session.commit()
        except :
            print sys.exc_info()[0]
            print "Exception in TestReader:"
            print '-' * 60
            traceback.print_exc(file=sys.stdout)
            print '-' * 60

            session.rollback(true)
            raise

processor = CompressFlowFile()
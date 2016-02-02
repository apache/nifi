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
from callbacks import ReadFirstLine
from org.apache.nifi.processor import Processor
from org.apache.nifi.processor import Relationship

class ReadContentAndStoreAsAttribute(Processor) :
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
        pass

    def onPropertyModified(self, descriptor, newValue, oldValue) :
        pass

    def onTrigger(self, context, sessionFactory) :
        session = sessionFactory.createSession()
        try :
            # ensure there is work to do
            flowfile = session.get()
            if flowfile is None :
                return

            reader = ReadFirstLine()
            session.read(flowfile, reader);

            # set an attribute
            flowfile = session.putAttribute(flowfile, "from-content", reader.getLine())
            # transfer
            session.transfer(flowfile, self.__rel_success)
            session.commit()
        except :
            print sys.exc_info()[0]
            print "Exception in TestReader:"
            print '-' * 60
            traceback.print_exc(file=sys.stdout)
            print '-' * 60

            session.rollback(True)
            raise

processor = ReadContentAndStoreAsAttribute()
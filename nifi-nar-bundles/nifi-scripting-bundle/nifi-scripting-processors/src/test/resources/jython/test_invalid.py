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

from org.apache.nifi.processor import Processor
from org.apache.nifi.components import ValidationResult

class AlwaysInvalid(Processor) :

    def __init__(self) :
        pass

    def initialize(self, context) :
        pass

    def getRelationships(self) :
        pass

    def validate(self, context) :
        error = ValidationResult.Builder().subject("Processor Error").valid(False).explanation("Never valid.").build()
        return [error]

    def getPropertyDescriptors(self) :
        pass

    def onPropertyModified(self, descriptor, newValue, oldValue) :
        pass

    def onTrigger(self, context, sessionFactory) :
        pass

processor = AlwaysInvalid()
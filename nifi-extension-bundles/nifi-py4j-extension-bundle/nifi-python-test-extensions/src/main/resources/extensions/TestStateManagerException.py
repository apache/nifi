#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from nifiapi.componentstate import Scope, StateManager, StateException
from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult

class TestStateManagerException(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = '''A Python source processor that uses StateManager.'''
        tags = ['text', 'test', 'python', 'source']

    def __init__(self, **kwargs):
        pass

    def create(self, context):
        state_manager = context.getStateManager()
        flowfile_attributes = None
        try:
            new_state = {'state_key_1': 'state_value_1'}
            state_manager.setState(new_state, Scope.CLUSTER)
        except StateException as state_exception:
            flowfile_attributes = {'exception_msg': str(state_exception)}

        return FlowFileSourceResult(relationship='success', attributes=flowfile_attributes, contents='Output FlowFile Contents')
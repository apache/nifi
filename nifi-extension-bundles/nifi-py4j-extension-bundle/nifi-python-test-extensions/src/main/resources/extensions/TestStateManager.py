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
from nifiapi.properties import PropertyDescriptor, StandardValidators, PropertyDependency

class TestStateManager(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = '''A Python source processor that uses StateManager.'''
        tags = ['text', 'test', 'python', 'source']

    METHOD_TO_TEST = PropertyDescriptor(
        name='StateManager Method To Test',
        description='''The name of StateManager's method that should be tested.''',
        required=True
    )

    property_descriptors = [METHOD_TO_TEST]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def create(self, context):
        method_to_test = context.getProperty(self.METHOD_TO_TEST).getValue()
        flowfile_attributes = None
        state_manager = context.getStateManager()

        match method_to_test.lower():
            case 'setstate':
                new_state = {'state_key_1': 'state_value_1'}
                state_manager.setState(new_state, Scope.CLUSTER)
            case 'getstate':
                flowfile_attributes = state_manager.getState(Scope.CLUSTER).toMap()
            case 'replace':
                old_state = state_manager.getState(Scope.CLUSTER)
                new_state = {'state_key_2': 'state_value_2'}
                state_manager.replace(old_state, new_state, Scope.CLUSTER)
            case 'clear':
                state_manager.clear(Scope.CLUSTER)
            case _:
                pass

        return FlowFileSourceResult(relationship='success', attributes=flowfile_attributes, contents='Output FlowFile Contents')
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

from nifiapi.properties import PropertyDescriptor
from nifiapi.properties import StandardValidators
from nifiapi.properties import ExpressionLanguageScope
from nifiapi.recordtransform import RecordTransformResult
from nifiapi.recordtransform import RecordTransform

class SetRecordField(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, **kwargs):
        super().__init__()

    def transform(self, context, record, schema, attributemap):
        # Update dictionary based on the dynamic properties provided by user
        for key in context.getProperties().keys():
            if not key.dynamic:
                continue

            propname = key.name
            record[propname] = context.getProperty(propname).evaluateAttributeExpressions(attributemap).getValue()

        # Determine the partition
        if 'group' in record:
            partition = {'group': record['group']}
        else:
            partition = None

        # Return the result
        return RecordTransformResult(record=record, relationship ='success', partition=partition)


    def getDynamicPropertyDescriptor(self, name):
        return PropertyDescriptor(
            name=name,
            description="Specifies the value to set for the '" + name + "' field",
            expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
            validators = [StandardValidators.ALWAYS_VALID]
        )

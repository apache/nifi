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

from nifiapi.documentation import use_case, multi_processor_use_case, ProcessorConfiguration

@use_case(description="First Use Case",
          notes="First Note",
          keywords=["A", "B"],
          configuration="""This Processor has no configuration.""")
@use_case(description="Second Use Case",
          notes="Another Note",
          keywords=["C", "B"],
          configuration="""No config.""")
@multi_processor_use_case(description="Multi Processor Use Case",
          notes="Note #1",
          keywords=["D", "E"],
          configurations=[
              ProcessorConfiguration(processor_type="OtherProcessor",
                                     configuration="No config "
                                                   "necessary."),
              ProcessorConfiguration(processor_type="DummyProcessor",
                                     configuration="None.")
          ])
class DummyProcessor:
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        description = "Fake Processor"
        tags = ["tag1", "tag2"]

    def __init__(self):
        pass

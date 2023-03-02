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

import pandas as pd
import io
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class ConvertCsvToExcel(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        dependencies = ['pandas', 'xlsxwriter']
        version = '0.0.1-SNAPSHOT'
        description = 'Converts a CSV file into a Microsoft Excel file'
        tags = ['csv', 'excel']

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        csv_df = pd.read_csv(io.BytesIO(flowFile.getContentsAsBytes()))
        bytesIo = io.BytesIO()
        writer = pd.ExcelWriter(bytesIo, engine='xlsxwriter')
        csv_df.to_excel(writer, index=False)
        writer.close()

        return FlowFileTransformResult(relationship = "success", contents = bytesIo.getvalue())


    def getPropertyDescriptors(self):
        return []
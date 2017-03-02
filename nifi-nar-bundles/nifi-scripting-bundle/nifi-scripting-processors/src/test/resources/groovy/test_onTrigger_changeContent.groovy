/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
def flowFile = session.get();
if (flowFile == null) {
    return;
}
def selectedColumns = ''

flowFile = session.write(flowFile,
        { inputStream, outputStream ->
            String line

            final BufferedReader inReader = new BufferedReader(new InputStreamReader(inputStream, 'UTF-8'))
            line = inReader.readLine()
            String[] header = line?.split(',')
            selectedColumns = "${header[2]},${header[3]}"

            while (line = inReader.readLine()) {
                String[] cols = line.split(',')
                // Select/project cols
                outputStream.write("${cols[2].capitalize()} ${cols[3].capitalize()}\n".getBytes('UTF-8'))
            }
        } as StreamCallback)

flowFile = session?.putAttribute(flowFile, "selected.columns", selectedColumns)
flowFile = session?.putAttribute(flowFile, "filename", "split_cols.txt")
session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
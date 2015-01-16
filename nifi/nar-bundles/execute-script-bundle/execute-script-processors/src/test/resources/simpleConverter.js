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
with (Scripting) {

    var instance = new ConverterScript({
        convert: function (input) {
            var buffReader = new java.io.BufferedReader(new java.io.InputStreamReader(input));
            instance.createFlowFile("firstLine", Script.FAIL_RELATIONSHIP, function (output) {
                var out = new java.io.BufferedWriter(new java.io.OutputStreamWriter(output));
                var firstLine = buffReader.readLine();
                out.write(firstLine, 0, firstLine.length());
                out.flush();
                out.close();
            });

            instance.createFlowFile("otherLines", Script.SUCCESS_RELATIONSHIP, function (output) {
                var out = new java.io.BufferedWriter(new java.io.OutputStreamWriter(output));
                var line = buffReader.readLine();
                while (line != null) {
                    out.write(line, 0, line.length());
                    out.newLine();
                    line = buffReader.readLine();
                }
                out.flush();
                out.close();
            });
        }

    });
    logger.debug("Processor props" + properties)
}
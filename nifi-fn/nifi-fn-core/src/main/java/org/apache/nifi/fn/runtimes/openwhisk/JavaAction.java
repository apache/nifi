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
package org.apache.nifi.fn.runtimes.openwhisk;

import com.google.gson.JsonObject;
import org.apache.nifi.fn.bootstrap.InMemoryFlowFile;
import org.apache.nifi.fn.core.FnFlow;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.Queue;

public class JavaAction {

    //wsk action create nififn NiFiFn/target/NiFi-Fn-1.0-SNAPSHOT.jar --main org.apache.nifi.fn.runtimes.OpenWhisk.JavaAction#OpenWhiskJavaEntry
    //wsk action invoke -br nififn -p registryurl http://172.0.0.1:61080 -p bucket e53b8a0d-5c85-4fcd-912a-1c549a586c83 -p flow 6cf8277a-c402-4957-8623-0fa9890dd45d -p variable1 val1 -p variable2 val2
    public static JsonObject OpenWhiskJavaEntry(JsonObject args) {

        JsonObject result = new JsonObject();
        try {
            FnFlow flow = FnFlow.createAndEnqueueFromJSON(args);
            Queue<InMemoryFlowFile> output = new LinkedList<>();
            boolean successful = flow.runOnce(output);

            StringBuilder response = new StringBuilder();
            for (InMemoryFlowFile flowFile : output) {
                response.append("\n").append(flowFile);
            }

            result.addProperty("success", successful);
            result.addProperty("message", response.toString());
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            result.addProperty("success", false);
            result.addProperty("message", "Flow exception: " + ex.getMessage() + "--" + sw.toString());
        }
        return result;
    }
}

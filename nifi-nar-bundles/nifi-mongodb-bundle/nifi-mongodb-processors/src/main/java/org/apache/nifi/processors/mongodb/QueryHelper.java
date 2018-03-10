/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public interface QueryHelper {
    AllowableValue MODE_ONE_COMMIT = new AllowableValue("all-at-once", "Full Query Fetch",
            "Fetch the entire query result and then make it available to downstream processors.");
    AllowableValue MODE_MANY_COMMITS = new AllowableValue("streaming", "Stream Query Results",
            "As soon as the query start sending results to the downstream processors at regular intervals.");

    PropertyDescriptor OPERATION_MODE = new PropertyDescriptor.Builder()
            .name("mongo-operation-mode")
            .displayName("Operation Mode")
            .allowableValues(MODE_ONE_COMMIT, MODE_MANY_COMMITS)
            .defaultValue(MODE_ONE_COMMIT.getValue())
            .required(true)
            .description("This option controls when results are made available to downstream processors. If Stream Query Results is enabled, " +
                    "provenance will not be tracked relative to the input flowfile if an input flowfile is received and starts the query. In Stream Query Results mode " +
                    "errors will be handled by sending a new flowfile with the original content and attributes of the input flowfile to the failure " +
                    "relationship. Streaming should only be used if there is reliable connectivity between MongoDB and NiFi.")
            .addValidator(Validator.VALID)
            .build();

    default String readQuery(ProcessContext context, ProcessSession session, PropertyDescriptor queryProp, FlowFile input) throws IOException {
        String queryStr;

        if (context.getProperty(queryProp).isSet()) {
            queryStr = context.getProperty(queryProp).evaluateAttributeExpressions(input).getValue();
        } else if (!context.getProperty(queryProp).isSet() && input == null) {
            queryStr = "{}";
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            session.exportTo(input, out);
            out.close();
            queryStr = new String(out.toByteArray());
        }

        return queryStr;
    }
}

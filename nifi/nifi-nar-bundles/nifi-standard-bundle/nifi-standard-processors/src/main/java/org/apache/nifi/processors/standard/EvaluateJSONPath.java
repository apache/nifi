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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"JSON", "evaluate", "JSONPath"})
@CapabilityDescription("")
public class EvaluateJSONPath extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the JSONPath evaluation are written to the FlowFile content or a FlowFile attribute; if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one JSONPath may be specified, and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder().name("matched").description("FlowFiles are routed to this relationship when the JSONPath is successfully evaluated and the FlowFile is modified as a result").build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder().name("unmatched").description("FlowFiles are routed to this relationship when the JSONPath does not match the content of the FlowFile and the Destination is set to flowfile-content").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles are routed to this relationship when the JSONPath cannot be evaluated against the content of the FlowFile; for instance, if the FlowFile is not valid JSON").build();


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

    }

}

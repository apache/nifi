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
package org.apache.nifi.record.sink.lookup;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.service.lookup.AbstractSingleAttributeBasedControllerServiceLookup;

import java.io.IOException;
import java.util.Map;


@Tags({"record", "sink", "lookup"})
@CapabilityDescription("Provides a RecordSinkService that can be used to dynamically select another RecordSinkService. This service " +
        "requires an attribute named 'record.sink.name' to be passed in when asking for a connection, and will throw an exception " +
        "if the attribute is missing. The value of 'record.sink.name' will be used to select the RecordSinkService that has been " +
        "registered with that name. This will allow multiple RecordSinkServices to be defined and registered, and then selected " +
        "dynamically at runtime by tagging flow files with the appropriate 'record.sink.name' attribute. Note that this controller service " +
        "is not intended for use in reporting tasks that employ RecordSinkService instances, such as QueryNiFiReportingTask.")
@DynamicProperty(name = "The name to register the specified RecordSinkService", value = "The RecordSinkService",
        description = "If '" + RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE + "' attribute contains " +
                "the name of the dynamic property, then the RecordSinkService (registered in the value) will be selected.",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class RecordSinkServiceLookup
        extends AbstractSingleAttributeBasedControllerServiceLookup<RecordSinkService> implements RecordSinkService {

    public static final String RECORD_SINK_NAME_ATTRIBUTE = "record.sink.name";

    RecordSinkService recordSinkService;

    @Override
    protected String getLookupAttribute() {
        return RECORD_SINK_NAME_ATTRIBUTE;
    }

    @Override
    public Class<RecordSinkService> getServiceType() {
        return RecordSinkService.class;
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        try {
            RecordSinkService recordSink = lookupService(attributes);
            if (recordSinkService != recordSink) {
                // Save for later reset(), and do a reset now since it has changed
                recordSinkService = recordSink;
                recordSinkService.reset();
            }
            return recordSinkService.sendData(recordSet, attributes, sendZeroResults);
        } catch (ProcessException pe) {
            // Lookup was unsuccessful, wrap the exception in an IOException to honor the contract
            throw new IOException(pe);
        }
    }

    @Override
    public void reset() {
        // By convention, calling reset() before sendData should be a no-op, so we can just delegate the reset() call after sendData() has been called once
        if (recordSinkService != null) {
            recordSinkService.reset();
        }
    }
}

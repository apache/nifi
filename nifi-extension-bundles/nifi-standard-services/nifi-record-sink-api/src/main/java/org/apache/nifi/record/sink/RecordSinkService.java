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
package org.apache.nifi.record.sink;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.util.Map;

/**
 * Definition for a Controller Service that specifies a RecordWriter as well as some transport mechanism to write records to some sink.
 */
@Tags({"record", "send", "write"})
@CapabilityDescription("Specifies a Controller Service that specifies a Record Writer as well as some transport mechanism to write the records to some destination (external system, e.g.)")
public interface RecordSinkService extends ControllerService {

    /**
     * A standard Record Writer property for use in RecordSinkService implementations
     */
    PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-sink-record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    /**
     * Sends the record set to the RecordSinkService
     * @param recordSet The RecordSet to transmit
     * @param attributes Attributes associated with the RecordSet
     * @param sendZeroResults Whether to transmit empty record sets
     * @return a WriteResult object containing the number of records transmitted, as well as any metadata in the form of attributes
     * @throws IOException if any error occurs during transmission of the record set
     */
    WriteResult sendData(final RecordSet recordSet, final Map<String,String> attributes, final boolean sendZeroResults) throws IOException;

    /**
     * Resets the RecordSinkService. This is useful when the service uses the record set's schema in order to transmit the data correctly. If subsequent
     * RecordSets have different schemas, this can cause issues with schema handling. Calling reset() should perform operations such as clearing the schema
     * and any appropriate data related to possibly different RecordSets. The default implementation is a no-op
     */
    default void reset() {}
}

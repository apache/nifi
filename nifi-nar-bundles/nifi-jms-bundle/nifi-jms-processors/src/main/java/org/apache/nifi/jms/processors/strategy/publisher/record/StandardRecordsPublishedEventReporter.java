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
package org.apache.nifi.jms.processors.strategy.publisher.record;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.processors.strategy.publisher.EventReporter;
import org.apache.nifi.processor.ProcessSession;

public class StandardRecordsPublishedEventReporter implements EventReporter {

    public static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE = "Publish failed after %d successfully published records.";
    public static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER = "Successfully finished publishing previously failed records. Total record count: %d";
    public static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS = "Successfully published all records. Total record count: %d";

    private final String transitUri;

    public static StandardRecordsPublishedEventReporter of(String transitUri) {
        return new StandardRecordsPublishedEventReporter(transitUri);
    }

    public StandardRecordsPublishedEventReporter(String transitUri) {
        this.transitUri = transitUri;
    }

    @Override
    public void reportSuccessEvent(ProcessSession session, FlowFile flowFile, int processedRecords, long transmissionMillis) {
        session.getProvenanceReporter().send(
                flowFile,
                transitUri,
                String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS, processedRecords),
                transmissionMillis);
    }

    @Override
    public void reportRecoverEvent(ProcessSession session, FlowFile flowFile, int processedRecords, long transmissionMillis) {
        session.getProvenanceReporter().send(
                flowFile,
                transitUri,
                String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER, processedRecords),
                transmissionMillis);
    }

    @Override
    public void reportFailureEvent(ProcessSession session, FlowFile flowFile, int processedRecords, long transmissionMillis) {
        session.getProvenanceReporter().send(
                flowFile,
                transitUri,
                String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE, processedRecords),
                transmissionMillis);
    }
}

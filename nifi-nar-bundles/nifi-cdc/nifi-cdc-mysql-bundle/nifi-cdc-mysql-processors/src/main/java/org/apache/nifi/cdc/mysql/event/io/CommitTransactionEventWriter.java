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
package org.apache.nifi.cdc.mysql.event.io;


import org.apache.nifi.cdc.event.io.EventWriterConfiguration;
import org.apache.nifi.cdc.mysql.event.CommitTransactionEventInfo;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.io.IOException;

/**
 * A writer for events corresponding to the end (i.e. commit) of a MySQL transaction
 */
public class CommitTransactionEventWriter extends AbstractBinlogEventWriter<CommitTransactionEventInfo> {

    @Override
    public long writeEvent(ProcessSession session, String transitUri, CommitTransactionEventInfo eventInfo, long currentSequenceId,
                           Relationship relationship, EventWriterConfiguration eventWriterConfiguration) {
        long sequenceId = super.writeEvent(session, transitUri, eventInfo, currentSequenceId, relationship, eventWriterConfiguration);
        // If writing one transaction per flowfile, finish the flowfile here before committing the session
        if (oneTransactionPerFlowFile(eventWriterConfiguration)) {
            super.finishAndTransferFlowFile(session, eventWriterConfiguration, transitUri, sequenceId, eventInfo, relationship);
        }
        return sequenceId;
    }

    protected void writeJson(CommitTransactionEventInfo event) throws IOException {
        super.writeJson(event);
        if (event.getDatabaseName() != null) {
            jsonGenerator.writeStringField("database", event.getDatabaseName());
        } else {
            jsonGenerator.writeNullField("database");
        }
    }
}

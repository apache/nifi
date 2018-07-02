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

import org.apache.nifi.cdc.mysql.event.BinlogEventInfo;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.cdc.event.io.AbstractEventWriter;
import org.apache.nifi.processor.Relationship;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract base class for writing MYSQL binlog events into flow file(s), e.g.
 */
public abstract class AbstractBinlogEventWriter<T extends BinlogEventInfo> extends AbstractEventWriter<T> {

    protected void writeJson(T event) throws IOException {
        String gtidSet = event.getBinlogGtidSet();

        if (gtidSet == null) {
            jsonGenerator.writeStringField("binlog_filename", event.getBinlogFilename());
            jsonGenerator.writeNumberField("binlog_position", event.getBinlogPosition());
        } else {
            jsonGenerator.writeStringField("binlog_gtidset", event.getBinlogGtidSet());
        }
    }

    protected Map<String, String> getCommonAttributes(final long sequenceId, BinlogEventInfo eventInfo) {
        return new HashMap<String, String>() {
            {
                put(SEQUENCE_ID_KEY, Long.toString(sequenceId));
                put(CDC_EVENT_TYPE_ATTRIBUTE, eventInfo.getEventType());
                String gtidSet = eventInfo.getBinlogGtidSet();
                if (gtidSet == null) {
                    put(BinlogEventInfo.BINLOG_FILENAME_KEY, eventInfo.getBinlogFilename());
                    put(BinlogEventInfo.BINLOG_POSITION_KEY, Long.toString(eventInfo.getBinlogPosition()));
                } else {
                    put(BinlogEventInfo.BINLOG_GTIDSET_KEY, gtidSet);
                }
                put(CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
            }
        };
    }

    // Default implementation for binlog events
    @Override
    public long writeEvent(ProcessSession session, String transitUri, T eventInfo, long currentSequenceId, Relationship relationship) {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, (outputStream) -> {
            super.startJson(outputStream, eventInfo);
            writeJson(eventInfo);
            // Nothing in the body
            super.endJson();
        });
        flowFile = session.putAllAttributes(flowFile, getCommonAttributes(currentSequenceId, eventInfo));
        session.transfer(flowFile, relationship);
        session.getProvenanceReporter().receive(flowFile, transitUri);
        return currentSequenceId + 1;
    }
}

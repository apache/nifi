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

import org.apache.nifi.cdc.event.EventInfo;
import org.apache.nifi.cdc.event.io.EventWriterConfiguration;
import org.apache.nifi.cdc.event.io.FlowFileEventWriteStrategy;
import org.apache.nifi.cdc.mysql.event.BinlogEventInfo;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.cdc.event.io.AbstractEventWriter;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;

import java.io.IOException;
import java.io.OutputStream;
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
        return new HashMap<>() {
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
    public long writeEvent(ProcessSession session, String transitUri, T eventInfo, long currentSequenceId, Relationship relationship,
                           final EventWriterConfiguration eventWriterConfiguration) {
        FlowFile flowFile = eventWriterConfiguration.getCurrentFlowFile();
        if (flowFile == null) {
            flowFile = session.create();
            OutputStream flowFileOutputStream = session.write(flowFile);
            eventWriterConfiguration.setFlowFileOutputStream(flowFileOutputStream);
            eventWriterConfiguration.setCurrentFlowFile(flowFile);
            if (eventWriterConfiguration.getJsonGenerator() == null) {
                try {
                    jsonGenerator = createJsonGenerator(flowFileOutputStream);
                    eventWriterConfiguration.setJsonGenerator(jsonGenerator);
                } catch (IOException ioe) {
                    throw new FlowFileAccessException("Couldn't create JSON generator", ioe);
                }
            }
            if ((FlowFileEventWriteStrategy.N_EVENTS_PER_FLOWFILE.equals(eventWriterConfiguration.getFlowFileEventWriteStrategy())
                    && eventWriterConfiguration.getNumberOfEventsPerFlowFile() > 1)
                    || FlowFileEventWriteStrategy.ONE_TRANSACTION_PER_FLOWFILE.equals(eventWriterConfiguration.getFlowFileEventWriteStrategy())) {
                try {
                    jsonGenerator.writeStartArray();
                } catch (IOException ioe) {
                    throw new FlowFileAccessException("Couldn't write start of event array", ioe);
                }
            }
        }
        jsonGenerator = eventWriterConfiguration.getJsonGenerator();

        OutputStream outputStream = eventWriterConfiguration.getFlowFileOutputStream();
        try {
            super.startJson(outputStream, eventInfo);
            writeJson(eventInfo);
            // Nothing in the body
            super.endJson();
        } catch (IOException ioe) {
            throw new FlowFileAccessException("Couldn't write start of event array", ioe);
        }

        eventWriterConfiguration.incrementNumberOfEventsWritten();

        // Check if it is time to finish the FlowFile
        if (FlowFileEventWriteStrategy.N_EVENTS_PER_FLOWFILE.equals(eventWriterConfiguration.getFlowFileEventWriteStrategy())
                && eventWriterConfiguration.getNumberOfEventsWritten() == eventWriterConfiguration.getNumberOfEventsPerFlowFile()) {
            flowFile = finishAndTransferFlowFile(eventWriterConfiguration, transitUri, currentSequenceId, eventInfo, relationship);
        }
        eventWriterConfiguration.setCurrentFlowFile(flowFile);
        return currentSequenceId + 1;
    }

    public FlowFile finishAndTransferFlowFile(final EventWriterConfiguration eventWriterConfiguration, final String transitUri, final long seqId,
                                              final BinlogEventInfo eventInfo, final Relationship relationship) {
        // If writing multiple events, end the array
        if (eventWriterConfiguration.getNumberOfEventsWritten() > 1
                || FlowFileEventWriteStrategy.ONE_TRANSACTION_PER_FLOWFILE.equals(eventWriterConfiguration.getFlowFileEventWriteStrategy())) {
            try {
                jsonGenerator.writeEndArray();
            } catch (IOException ioe) {
                throw new FlowFileAccessException("Couldn't write end of event array", ioe);
            }
        }
        try {
            endFile();
            eventWriterConfiguration.setJsonGenerator(null);
            eventWriterConfiguration.getFlowFileOutputStream().close();
        } catch (IOException ioe) {
            throw new FlowFileAccessException("Couldn't flush and close file", ioe);
        }
        FlowFile flowFile = eventWriterConfiguration.getCurrentFlowFile();
        ProcessSession session = eventWriterConfiguration.getWorkingSession();
        if (session == null && flowFile == null) {
            throw new FlowFileAccessException("No open FlowFile or ProcessSession to write to");
        }
        flowFile = session.putAllAttributes(flowFile, getCommonAttributes(seqId, eventInfo));
        session.transfer(flowFile, relationship);
        session.getProvenanceReporter().receive(flowFile, transitUri);
        eventWriterConfiguration.setNumberOfEventsWritten(0);
        eventWriterConfiguration.setCurrentFlowFile(null);
        return null;
    }

    protected FlowFile configureEventWriter(final EventWriterConfiguration eventWriterConfiguration, final ProcessSession session, final EventInfo eventInfo) {
        FlowFile flowFile = eventWriterConfiguration.getCurrentFlowFile();
        if (flowFile == null) {
            flowFile = session.create();
            OutputStream flowFileOutputStream = session.write(flowFile);
            eventWriterConfiguration.setFlowFileOutputStream(flowFileOutputStream);
            if (eventWriterConfiguration.getJsonGenerator() == null) {
                try {
                    jsonGenerator = createJsonGenerator(flowFileOutputStream);
                    eventWriterConfiguration.setJsonGenerator(jsonGenerator);
                } catch (IOException ioe) {
                    throw new FlowFileAccessException("Couldn't create JSON generator", ioe);
                }
            }
            if ((FlowFileEventWriteStrategy.N_EVENTS_PER_FLOWFILE.equals(eventWriterConfiguration.getFlowFileEventWriteStrategy())
                    && eventWriterConfiguration.getNumberOfEventsPerFlowFile() > 1)
                    || FlowFileEventWriteStrategy.ONE_TRANSACTION_PER_FLOWFILE.equals(eventWriterConfiguration.getFlowFileEventWriteStrategy())) {
                try {
                    jsonGenerator.writeStartArray();
                } catch (IOException ioe) {
                    throw new FlowFileAccessException("Couldn't write start of event array", ioe);
                }
            }
        }
        jsonGenerator = eventWriterConfiguration.getJsonGenerator();
        eventWriterConfiguration.setCurrentFlowFile(flowFile);
        return flowFile;
    }
}

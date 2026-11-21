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
package org.apache.nifi.cdc.mongodb.processors;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.nifi.cdc.mongodb.event.EventMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.bson.BsonDocument;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * The change events read by one trigger, written as one FlowFile. The FlowFile is created with the first event,
 * so a trigger that finds nothing on the stream leaves no trace.
 */
class EventBatch {

    private final ProcessSession session;
    private final RecordSetWriterFactory writerFactory;
    private final EventMapper eventMapper;
    private final ComponentLog logger;
    private final String transitUri;
    private final Map<String, String> namespaceAttributes;

    private FlowFile flowFile;
    private OutputStream outputStream;
    private RecordSetWriter writer;
    private int eventCount;
    private String firstResumeToken;
    private String lastResumeToken;
    private Long lastWallTimeMillis;

    EventBatch(final ProcessSession session, final RecordSetWriterFactory writerFactory, final EventMapper eventMapper,
               final ComponentLog logger, final String transitUri, final Map<String, String> namespaceAttributes) {
        this.session = session;
        this.writerFactory = writerFactory;
        this.eventMapper = eventMapper;
        this.logger = logger;
        this.transitUri = transitUri;
        this.namespaceAttributes = namespaceAttributes;
    }

    boolean isEmpty() {
        return eventCount == 0;
    }

    int getEventCount() {
        return eventCount;
    }

    String getLastResumeToken() {
        return lastResumeToken;
    }

    void write(final ChangeStreamDocument<BsonDocument> event) throws IOException {
        write(eventMapper.map(event));

        final String resumeToken = EventMapper.resumeTokenData(event.getResumeToken());
        if (firstResumeToken == null) {
            firstResumeToken = resumeToken;
        }
        lastResumeToken = resumeToken;
        lastWallTimeMillis = event.getWallTime() == null ? null : event.getWallTime().getValue();
    }

    /**
     * A record that is not a change event, so it carries no resume token: a document read by the initial snapshot.
     */
    void write(final Record record) throws IOException {
        if (writer == null) {
            begin();
        }
        writer.write(record);
        eventCount++;
    }

    void transfer(final Relationship relationship) throws IOException {
        final WriteResult result;
        final OutputStream currentStream = outputStream;
        final RecordSetWriter currentWriter = writer;
        // resources close in reverse order: the writer must flush into the stream before the stream is closed
        try (currentStream; currentWriter) {
            result = currentWriter.finishRecordSet();
        }

        final Map<String, String> attributes = new HashMap<>(result.getAttributes());
        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
        attributes.put(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, Integer.toString(result.getRecordCount()));
        // The records of the initial snapshot are not change events, so they have neither token nor wall clock time.
        if (firstResumeToken != null) {
            attributes.put(CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, firstResumeToken);
            attributes.put(CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, lastResumeToken);
        }
        if (lastWallTimeMillis != null) {
            attributes.put(CaptureChangeMongoDB.ATTRIBUTE_LAG_MILLIS, Long.toString(Math.max(0L, System.currentTimeMillis() - lastWallTimeMillis)));
        }
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.getProvenanceReporter().receive(flowFile, transitUri);
        session.transfer(flowFile, relationship);
    }

    /**
     * Discard everything written so far. Never throws, so that callers can rely on it in error handling.
     */
    void rollback() {
        if (writer != null) {
            final OutputStream currentStream = outputStream;
            final RecordSetWriter currentWriter = writer;
            try (currentStream; currentWriter) {
                // closing releases the content claim; the session rollback removes the FlowFile
                logger.debug("Discarding {} change events written before the failure", eventCount);
            } catch (final Exception e) {
                logger.debug("Closing Record Writer failed during rollback", e);
            }
        }
        try {
            session.rollback();
        } catch (final Exception e) {
            logger.warn("Rolling back session failed", e);
        }
    }

    private void begin() throws IOException {
        flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, namespaceAttributes);
        outputStream = session.write(flowFile);
        try {
            final RecordSchema schema = writerFactory.getSchema(namespaceAttributes, eventMapper.getEventSchema());
            writer = writerFactory.createWriter(logger, schema, outputStream, flowFile);
            writer.beginRecordSet();
        } catch (final Exception e) {
            outputStream.close();
            outputStream = null;
            throw new IOException("Record Writer initialization failed", e);
        }
    }
}

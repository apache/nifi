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

package org.apache.nifi.processors.druid;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import org.apache.nifi.controller.api.druid.DruidTranquilityService;
import org.apache.nifi.expression.ExpressionLanguageScope;

import com.metamx.tranquility.tranquilizer.Tranquilizer;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import scala.runtime.BoxedUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"druid", "timeseries", "olap", "ingest", "put", "record"})
@CapabilityDescription("Sends records to Druid for Indexing. Leverages Druid Tranquility Controller service.")
@WritesAttribute(attribute = "record.count", description = "The number of messages that were sent to Druid for this FlowFile. FlowFiles on the success relationship will have a value "
        + "of this attribute that indicates the number of records successfully processed by Druid, and the FlowFile content will be only the successful records. This behavior applies "
        + "to the failure and dropped relationships as well.")
public class PutDruidRecord extends AbstractSessionFactoryProcessor {

    static final String RECORD_COUNT = "record.count";

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("putdruid-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("putdruid-record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data to outgoing relationships.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor DRUID_TRANQUILITY_SERVICE = new PropertyDescriptor.Builder()
            .name("putdruid-tranquility-service")
            .displayName("Tranquility Service")
            .description("Tranquility Service to use for sending events to Druid.")
            .required(true)
            .identifiesControllerService(DruidTranquilityService.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to this relationship when they are successfully processed by Druid")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when they cannot be parsed or otherwise processed by Druid")
            .build();

    static final Relationship REL_DROPPED = new Relationship.Builder()
            .name("dropped")
            .description("FlowFiles are routed to this relationship when they are outside of the configured time window, timestamp format is invalid, ect...")
            .build();


    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER_FACTORY);
        properties.add(RECORD_WRITER_FACTORY);
        properties.add(DRUID_TRANQUILITY_SERVICE);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_DROPPED);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Parses the record(s), converts each to a Map, and sends via Tranquility to the Druid Indexing Service
     *
     * @param context The process context
     * @param session The process session
     */
    @SuppressWarnings("unchecked")
    private void processFlowFile(ProcessContext context, final ProcessSession session) {
        final ComponentLog log = getLogger();

        // Get handle on Druid Tranquility session
        DruidTranquilityService tranquilityController = context.getProperty(DRUID_TRANQUILITY_SERVICE)
                .asControllerService(DruidTranquilityService.class);
        Tranquilizer<Map<String, Object>> tranquilizer = tranquilityController.getTranquilizer();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Create the outgoing flow files and output streams
        FlowFile droppedFlowFile = session.create(flowFile);
        final AtomicInteger droppedFlowFileCount = new AtomicInteger(0);
        FlowFile failedFlowFile = session.create(flowFile);
        final AtomicInteger failedFlowFileCount = new AtomicInteger(0);
        FlowFile successfulFlowFile = session.create(flowFile);
        final AtomicInteger successfulFlowFileCount = new AtomicInteger(0);

        final AtomicInteger recordWriteErrors = new AtomicInteger(0);

        int recordCount = 0;
        final OutputStream droppedOutputStream = session.write(droppedFlowFile);
        final RecordSetWriter droppedRecordWriter;
        final OutputStream failedOutputStream = session.write(failedFlowFile);
        final RecordSetWriter failedRecordWriter;
        final OutputStream successfulOutputStream = session.write(successfulFlowFile);
        final RecordSetWriter successfulRecordWriter;
        try (final InputStream in = session.read(flowFile)) {

            final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                    .asControllerService(RecordReaderFactory.class);
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);

            final Map<String, String> attributes = flowFile.getAttributes();

            final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger());
            final RecordSchema outSchema = writerFactory.getSchema(attributes, reader.getSchema());
            droppedRecordWriter = writerFactory.createWriter(log, outSchema, droppedOutputStream);
            droppedRecordWriter.beginRecordSet();
            failedRecordWriter = writerFactory.createWriter(log, outSchema, failedOutputStream);
            failedRecordWriter.beginRecordSet();
            successfulRecordWriter = writerFactory.createWriter(log, outSchema, successfulOutputStream);
            successfulRecordWriter.beginRecordSet();

            Record r;
            while ((r = reader.nextRecord()) != null) {
                final Record record = r;
                recordCount++;
                // Convert each Record to HashMap and send to Druid
                Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(r, RecordFieldType.RECORD.getRecordDataType(r.getSchema()));

                log.debug("Tranquilizer Status: {}", new Object[]{tranquilizer.status().toString()});
                // Send data element to Druid asynchronously
                Future<BoxedUnit> future = tranquilizer.send(contentMap);
                log.debug("Sent Payload to Druid: {}", new Object[]{contentMap});

                // Wait for Druid to call back with status
                future.addEventListener(new FutureEventListener<Object>() {
                    @Override
                    public void onFailure(Throwable cause) {
                        if (cause instanceof MessageDroppedException) {
                            // This happens when event timestamp targets a Druid Indexing task that has closed (Late Arriving Data)
                            log.debug("Record Dropped due to MessageDroppedException: {}, transferring record to dropped.", new Object[]{cause.getMessage()}, cause);
                            try {
                                synchronized (droppedRecordWriter) {
                                    droppedRecordWriter.write(record);
                                    droppedRecordWriter.flush();
                                    droppedFlowFileCount.incrementAndGet();
                                }
                            } catch (final IOException ioe) {
                                log.error("Error transferring record to dropped, this may result in data loss.", new Object[]{ioe.getMessage()}, ioe);
                                recordWriteErrors.incrementAndGet();
                            }

                        } else {
                            log.error("FlowFile Processing Failed due to: {}", new Object[]{cause.getMessage()}, cause);
                            try {
                                synchronized (failedRecordWriter) {
                                    failedRecordWriter.write(record);
                                    failedRecordWriter.flush();
                                    failedFlowFileCount.incrementAndGet();
                                }
                            } catch (final IOException ioe) {
                                log.error("Error transferring record to failure, this may result in data loss.", new Object[]{ioe.getMessage()}, ioe);
                                recordWriteErrors.incrementAndGet();
                            }
                        }
                    }

                    @Override
                    public void onSuccess(Object value) {
                        log.debug(" FlowFile Processing Success: {}", new Object[]{value.toString()});
                        try {
                            synchronized (successfulRecordWriter) {
                                successfulRecordWriter.write(record);
                                successfulRecordWriter.flush();
                                successfulFlowFileCount.incrementAndGet();
                            }
                        } catch (final IOException ioe) {
                            log.error("Error transferring record to success, this may result in data loss. "
                                    + "However the record was successfully processed by Druid", new Object[]{ioe.getMessage()}, ioe);
                            recordWriteErrors.incrementAndGet();
                        }
                    }
                });

            }

        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            log.error("FlowFile Processing Failed due to: {}", new Object[]{e.getMessage()}, e);
            // The FlowFile will be obtained and the error logged below, when calling publishResult.getFailedFlowFiles()
            flowFile = session.putAttribute(flowFile, RECORD_COUNT, Integer.toString(recordCount));
            session.transfer(flowFile, REL_FAILURE);
            try {
                droppedOutputStream.close();
                session.remove(droppedFlowFile);
            } catch (IOException ioe) {
                log.error("Error closing output stream for FlowFile with dropped records.", ioe);
            }
            try {
                failedOutputStream.close();
                session.remove(failedFlowFile);
            } catch (IOException ioe) {
                log.error("Error closing output stream for FlowFile with failed records.", ioe);
            }
            try {
                successfulOutputStream.close();
                session.remove(successfulFlowFile);
            } catch (IOException ioe) {
                log.error("Error closing output stream for FlowFile with successful records.", ioe);
            }
            session.commit();
            return;
        }

        if (recordCount == 0) {
            // Send original (empty) flow file to success, remove the rest
            flowFile = session.putAttribute(flowFile, RECORD_COUNT, "0");
            session.transfer(flowFile, REL_SUCCESS);
            try {
                droppedOutputStream.close();
                session.remove(droppedFlowFile);
            } catch (IOException ioe) {
                log.error("Error closing output stream for FlowFile with dropped records.", ioe);
            }
            try {
                failedOutputStream.close();
                session.remove(failedFlowFile);
            } catch (IOException ioe) {
                log.error("Error closing output stream for FlowFile with failed records.", ioe);
            }
            try {
                successfulOutputStream.close();
                session.remove(successfulFlowFile);
            } catch (IOException ioe) {
                log.error("Error closing output stream for FlowFile with successful records.", ioe);
            }
        } else {

            // Wait for all the records to finish processing
            while (recordCount != (droppedFlowFileCount.get() + failedFlowFileCount.get() + successfulFlowFileCount.get() + recordWriteErrors.get())) {
                Thread.yield();
            }

            // Send partitioned flow files out to their relationships (or remove them if empty)

            try {
                droppedRecordWriter.finishRecordSet();
                droppedRecordWriter.close();
            } catch (IOException ioe) {
                log.error("Error closing FlowFile with dropped records: {}", new Object[]{ioe.getMessage()}, ioe);
                session.rollback();
                throw new ProcessException(ioe);
            }
            if (droppedFlowFileCount.get() > 0) {
                droppedFlowFile = session.putAttribute(droppedFlowFile, RECORD_COUNT, Integer.toString(droppedFlowFileCount.get()));
                session.transfer(droppedFlowFile, REL_DROPPED);
            } else {
                session.remove(droppedFlowFile);
            }

            try {
                failedRecordWriter.finishRecordSet();
                failedRecordWriter.close();
            } catch (IOException ioe) {
                log.error("Error closing FlowFile with failed records: {}", new Object[]{ioe.getMessage()}, ioe);
                session.rollback();
                throw new ProcessException(ioe);
            }
            if (failedFlowFileCount.get() > 0) {
                failedFlowFile = session.putAttribute(failedFlowFile, RECORD_COUNT, Integer.toString(failedFlowFileCount.get()));
                session.transfer(failedFlowFile, REL_FAILURE);
            } else {
                session.remove(failedFlowFile);
            }

            try {
                successfulRecordWriter.finishRecordSet();
                successfulRecordWriter.close();
            } catch (IOException ioe) {
                log.error("Error closing FlowFile with successful records: {}", new Object[]{ioe.getMessage()}, ioe);
                session.rollback();
                throw new ProcessException(ioe);
            }
            if (successfulFlowFileCount.get() > 0) {
                successfulFlowFile = session.putAttribute(successfulFlowFile, RECORD_COUNT, Integer.toString(successfulFlowFileCount.get()));
                session.transfer(successfulFlowFile, REL_SUCCESS);
                session.getProvenanceReporter().send(successfulFlowFile, tranquilityController.getTransitUri());
            } else {
                session.remove(successfulFlowFile);
            }

            session.remove(flowFile);
        }

        session.commit();
    }

    public void onTrigger(ProcessContext context, ProcessSessionFactory factory) throws ProcessException {
        final ProcessSession session = factory.createSession();
        processFlowFile(context, session);
    }
}
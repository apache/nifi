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

package org.apache.nifi.processors.cassandra;

import org.apache.nifi.cassandra.api.CqlFieldInfo;
import org.apache.nifi.cassandra.api.CqlQueryCallback;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.processors.cassandra.AbstractCQLProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.cassandra.ExecuteCQLQueryRecord.REL_ORIGINAL;

public class ExecuteCQLQueryCallback implements CqlQueryCallback {
    private final ProcessSession session;
    private RecordSetWriterFactory writerFactory;
    private RecordSetWriter recordWriter;
    private ComponentLog logger;

    private long currentIndex = 0;
    private long rowsPerFlowFile;
    private long flowFilesPerBatch;
    private FlowFile parentFlowFile;

    private boolean commitImmediately;
    private final List<FlowFile> flowFileBatch;
    private FlowFile currentFlowFile;

    private long recordsProcessed;
    private int fragmentIndex;
    private UUID fragmentId;

    public ExecuteCQLQueryCallback(FlowFile parentFlowFile,
                                   RecordSetWriterFactory writerFactory,
                                   ProcessSession session,
                                   ComponentLog logger,
                                   long rowsPerFlowfile,
                                   long flowFilesPerBatch) {
        this.parentFlowFile = parentFlowFile;
        this.writerFactory = writerFactory;
        this.session = session;
        this.logger = logger;

        this.commitImmediately = flowFilesPerBatch > 0;
        this.rowsPerFlowFile = rowsPerFlowfile;
        this.flowFilesPerBatch = flowFilesPerBatch;

        this.flowFileBatch = new ArrayList<>();
        this.recordsProcessed = 0;
        this.fragmentIndex = 0;
        this.fragmentId = UUID.randomUUID();
    }

    private void updateFlowFileAttributes() {
        Map<String, String> attributes = Map.of(FragmentAttributes.FRAGMENT_COUNT.key(), String.valueOf(recordsProcessed),
                FragmentAttributes.FRAGMENT_ID.key(), fragmentId.toString(),
                FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex++),
                "mime.type", recordWriter.getMimeType());

        this.currentFlowFile = session.putAllAttributes(currentFlowFile, attributes);
        this.recordsProcessed = 0;
        flowFileBatch.add(currentFlowFile);
    }

    private void initWriter(RecordSchema schema) {
        try {
            if (recordWriter != null) {
                recordWriter.finishRecordSet();
                recordWriter.close();

                updateFlowFileAttributes();

                if (commitImmediately && flowFileBatch.size() == flowFilesPerBatch) {
                    session.transfer(flowFileBatch, REL_SUCCESS);

                    if (parentFlowFile != null) {
                        session.transfer(parentFlowFile, REL_ORIGINAL);
                        parentFlowFile = null;
                    }

                    session.commitAsync();
                    flowFileBatch.clear();
                }
            }

            currentFlowFile = session.create();

            recordWriter = writerFactory.createWriter(logger, schema, session.write(currentFlowFile), currentFlowFile);
            recordWriter.beginRecordSet();
        } catch (Exception ex) {
            flowFileBatch.forEach(session::remove);

            throw new ProcessException("Error creating record writer", ex);
        }
    }

    @Override
    public void receive(long rowNumber,
                        org.apache.nifi.serialization.record.Record result, List<CqlFieldInfo> fields, boolean hasMore) {
        if (recordWriter == null || ++currentIndex % rowsPerFlowFile == 0) {
            initWriter(result.getSchema());
        }

        try {
            recordWriter.write(result);
            recordsProcessed++;

            if (!hasMore) {
                recordWriter.finishRecordSet();
                recordWriter.close();

                updateFlowFileAttributes();

                if (parentFlowFile != null) {
                    session.transfer(parentFlowFile, REL_ORIGINAL);
                }

                session.transfer(flowFileBatch, REL_SUCCESS);
                flowFileBatch.clear();
            }

        } catch (Exception ex) {
            try {
                recordWriter.close();
            } catch (Exception e) {
                logger.error("Error closing record writer", e);
            }

            if (!flowFileBatch.isEmpty()) {
                flowFileBatch.forEach(session::remove);
            }

            throw new ProcessException("Error writing record", ex);
        }
    }
}

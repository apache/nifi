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

package org.apache.nifi.processors.cql;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.service.cql.api.service.CQLQueryCallback;

import java.io.Closeable;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.processors.cql.AbstractCQLProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.cql.ExecuteCQLQueryRecord.REL_ORIGINAL;

public class ExecuteCQLQueryCallback implements CQLQueryCallback {
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
        this.fragmentIndex = 0;
        this.fragmentId = UUID.randomUUID();
    }

    /**
     * @return true if no rows were ever received, meaning no FlowFiles were created. The caller is responsible
     * for routing the incoming FlowFile (if any) in that case, since this callback never gets a chance to.
     */
    public boolean isEmpty() {
        return recordWriter == null;
    }

    private void updateFlowFileAttributes() {
        Map<String, String> attributes = Map.of(
                FragmentAttributes.FRAGMENT_ID.key(), fragmentId.toString(),
                FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex++),
                "mime.type", recordWriter.getMimeType());

        this.currentFlowFile = session.putAllAttributes(currentFlowFile, attributes);
        flowFileBatch.add(currentFlowFile);
    }

    /**
     * fragment.count can only be known once the whole result set has been processed. When Output Batch Size
     * commits FlowFiles early, earlier FlowFiles are already released to REL_SUCCESS before the total is known,
     * so fragment.count is intentionally left unset in that mode, matching the OUTPUT_BATCH_SIZE property
     * description. Otherwise, every FlowFile in flowFileBatch is still held here, so the true total is known
     * and applied to all of them.
     */
    private void finalizeFragmentCounts() {
        if (!commitImmediately) {
            final String totalFragmentCount = String.valueOf(flowFileBatch.size());
            flowFileBatch.replaceAll(flowFile -> session.putAttribute(flowFile, FragmentAttributes.FRAGMENT_COUNT.key(), totalFragmentCount));
        }
    }

    /**
     * session.remove(FlowFile) refuses to remove a FlowFile with an open OutputStream/callback from
     * session.write(FlowFile), so any writer or stream tied to a FlowFile being discarded on an error path
     * must be closed first.
     */
    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ex) {
                logger.error("Error closing {}", closeable, ex);
            }
        }
    }

    private void removeUnfinishedFlowFiles() {
        flowFileBatch.remove(currentFlowFile);
        if (currentFlowFile != null) {
            session.remove(currentFlowFile);
        }
        flowFileBatch.forEach(session::remove);
        flowFileBatch.clear();
    }

    private void initWriter(RecordSchema schema) {
        if (recordWriter != null) {
            try {
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
            } catch (Exception ex) {
                closeQuietly(recordWriter);
                removeUnfinishedFlowFiles();

                throw new ProcessException("Error closing record writer", ex);
            }
        }

        final FlowFile newFlowFile = session.create();
        final OutputStream out = session.write(newFlowFile);
        try {
            recordWriter = writerFactory.createWriter(logger, schema, out, newFlowFile);
            recordWriter.beginRecordSet();
            currentFlowFile = newFlowFile;
        } catch (Exception ex) {
            closeQuietly(out);

            session.remove(newFlowFile);
            flowFileBatch.forEach(session::remove);
            flowFileBatch.clear();

            throw new ProcessException("Error creating record writer", ex);
        }
    }

    @Override
    public void receive(long rowNumber,
                        org.apache.nifi.serialization.record.Record result, boolean hasMore) {
        final boolean shouldRotateWriter = recordWriter == null
                || (rowsPerFlowFile > 0 && ++currentIndex % rowsPerFlowFile == 0);

        if (shouldRotateWriter) {
            initWriter(result.getSchema());
        }

        try {
            recordWriter.write(result);

            if (!hasMore) {
                recordWriter.finishRecordSet();
                recordWriter.close();

                updateFlowFileAttributes();
                finalizeFragmentCounts();

                if (parentFlowFile != null) {
                    session.transfer(parentFlowFile, REL_ORIGINAL);
                    parentFlowFile = null;
                }

                session.transfer(flowFileBatch, REL_SUCCESS);
                flowFileBatch.clear();
            }

        } catch (Exception ex) {
            closeQuietly(recordWriter);
            removeUnfinishedFlowFiles();

            throw new ProcessException("Error writing record", ex);
        }
    }

    @Override
    public void clear() {
        closeQuietly(recordWriter);
        removeUnfinishedFlowFiles();
    }

    @Override
    public boolean hasSentOriginal() {
        return parentFlowFile == null;
    }
}

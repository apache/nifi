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
package org.apache.nifi.provenance;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

import org.apache.nifi.provenance.serialization.CompressableRecordWriter;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.stream.io.DataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Deprecated in favor of SchemaRecordWriter
 */
@Deprecated
public class StandardRecordWriter extends CompressableRecordWriter implements RecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(StandardRecordWriter.class);
    public static final int SERIALIZATION_VERISON = 9;
    public static final String SERIALIZATION_NAME = "org.apache.nifi.provenance.PersistentProvenanceRepository";

    private final File file;


    public StandardRecordWriter(final File file, final TocWriter writer, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(file, writer, compressed, uncompressedBlockSize);
        logger.trace("Creating Record Writer for {}", file.getName());

        this.file = file;
    }

    public StandardRecordWriter(final OutputStream out, final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(out, tocWriter, compressed, uncompressedBlockSize);
        this.file = null;
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERISON;
    }

    @Override
    protected void writeHeader(long firstEventId, DataOutputStream out) throws IOException {
    }

    @Override
    protected void writeRecord(final ProvenanceEventRecord record, final long recordIdentifier, final DataOutputStream out) throws IOException {
        final ProvenanceEventType recordType = record.getEventType();

        out.writeLong(recordIdentifier);
        out.writeUTF(record.getEventType().name());
        out.writeLong(record.getEventTime());
        out.writeLong(record.getFlowFileEntryDate());
        out.writeLong(record.getEventDuration());
        out.writeLong(record.getLineageStartDate());

        writeNullableString(out, record.getComponentId());
        writeNullableString(out, record.getComponentType());
        writeUUID(out, record.getFlowFileUuid());
        writeNullableString(out, record.getDetails());

        // Write FlowFile attributes
        final Map<String, String> attrs = record.getPreviousAttributes();
        out.writeInt(attrs.size());
        for (final Map.Entry<String, String> entry : attrs.entrySet()) {
            writeLongString(out, entry.getKey());
            writeLongString(out, entry.getValue());
        }

        final Map<String, String> attrUpdates = record.getUpdatedAttributes();
        out.writeInt(attrUpdates.size());
        for (final Map.Entry<String, String> entry : attrUpdates.entrySet()) {
            writeLongString(out, entry.getKey());
            writeLongNullableString(out, entry.getValue());
        }

        // If Content Claim Info is present, write out a 'TRUE' followed by claim info. Else, write out 'false'.
        if (record.getContentClaimSection() != null && record.getContentClaimContainer() != null && record.getContentClaimIdentifier() != null) {
            out.writeBoolean(true);
            out.writeUTF(record.getContentClaimContainer());
            out.writeUTF(record.getContentClaimSection());
            out.writeUTF(record.getContentClaimIdentifier());
            if (record.getContentClaimOffset() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(record.getContentClaimOffset());
            }
            out.writeLong(record.getFileSize());
        } else {
            out.writeBoolean(false);
        }

        // If Previous Content Claim Info is present, write out a 'TRUE' followed by claim info. Else, write out 'false'.
        if (record.getPreviousContentClaimSection() != null && record.getPreviousContentClaimContainer() != null && record.getPreviousContentClaimIdentifier() != null) {
            out.writeBoolean(true);
            out.writeUTF(record.getPreviousContentClaimContainer());
            out.writeUTF(record.getPreviousContentClaimSection());
            out.writeUTF(record.getPreviousContentClaimIdentifier());
            if (record.getPreviousContentClaimOffset() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(record.getPreviousContentClaimOffset());
            }

            if (record.getPreviousFileSize() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(record.getPreviousFileSize());
            }
        } else {
            out.writeBoolean(false);
        }

        // write out the identifier of the destination queue.
        writeNullableString(out, record.getSourceQueueIdentifier());

        // Write type-specific info
        if (recordType == ProvenanceEventType.FORK || recordType == ProvenanceEventType.JOIN || recordType == ProvenanceEventType.CLONE || recordType == ProvenanceEventType.REPLAY) {
            writeUUIDs(out, record.getParentUuids());
            writeUUIDs(out, record.getChildUuids());
        } else if (recordType == ProvenanceEventType.RECEIVE) {
            writeNullableString(out, record.getTransitUri());
            writeNullableString(out, record.getSourceSystemFlowFileIdentifier());
        } else if (recordType == ProvenanceEventType.FETCH) {
            writeNullableString(out, record.getTransitUri());
        } else if (recordType == ProvenanceEventType.SEND) {
            writeNullableString(out, record.getTransitUri());
        } else if (recordType == ProvenanceEventType.ADDINFO) {
            writeNullableString(out, record.getAlternateIdentifierUri());
        } else if (recordType == ProvenanceEventType.ROUTE) {
            writeNullableString(out, record.getRelationship());
        }
    }

    protected void writeUUID(final DataOutputStream out, final String uuid) throws IOException {
        out.writeUTF(uuid);
    }

    protected void writeUUIDs(final DataOutputStream out, final Collection<String> list) throws IOException {
        if (list == null) {
            out.writeInt(0);
        } else {
            out.writeInt(list.size());
            for (final String value : list) {
                writeUUID(out, value);
            }
        }
    }

    protected void writeNullableString(final DataOutputStream out, final String toWrite) throws IOException {
        if (toWrite == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(toWrite);
        }
    }

    private void writeLongNullableString(final DataOutputStream out, final String toWrite) throws IOException {
        if (toWrite == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeLongString(out, toWrite);
        }
    }

    private void writeLongString(final DataOutputStream out, final String value) throws IOException {
        final byte[] bytes = value.getBytes("UTF-8");
        out.writeInt(bytes.length);
        out.write(bytes);
    }


    @Override
    public String toString() {
        return "StandardRecordWriter[file=" + file + "]";
    }
}

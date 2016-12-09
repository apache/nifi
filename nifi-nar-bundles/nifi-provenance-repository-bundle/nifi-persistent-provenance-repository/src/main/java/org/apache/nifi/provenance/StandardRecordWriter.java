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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.serialization.CompressableRecordWriter;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.TocWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Deprecated in favor of SchemaRecordWriter
 */
@Deprecated
public class StandardRecordWriter extends CompressableRecordWriter implements RecordWriter {

    public static final int MAX_ALLOWED_UTF_LENGTH = 65_535;

    private static final Logger logger = LoggerFactory.getLogger(StandardRecordWriter.class);
    public static final int SERIALIZATION_VERISON = 9;
    public static final String SERIALIZATION_NAME = "org.apache.nifi.provenance.PersistentProvenanceRepository";

    private final File file;


    public StandardRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(file, idGenerator, writer, compressed, uncompressedBlockSize);
        logger.trace("Creating Record Writer for {}", file.getName());

        this.file = file;
    }

    public StandardRecordWriter(final OutputStream out, final String storageLocation, final AtomicLong idGenerator, final TocWriter tocWriter,
        final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(out, storageLocation, idGenerator, tocWriter, compressed, uncompressedBlockSize);
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
        writeUTFLimited(out, record.getEventType().name(), "EventType");
        out.writeLong(record.getEventTime());
        out.writeLong(record.getFlowFileEntryDate());
        out.writeLong(record.getEventDuration());
        out.writeLong(record.getLineageStartDate());

        writeNullableString(out, record.getComponentId(), "ComponentId");
        writeNullableString(out, record.getComponentType(), "ComponentType");
        writeUUID(out, record.getFlowFileUuid());
        writeNullableString(out, record.getDetails(), "Details");

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
            writeUTFLimited(out, record.getContentClaimContainer(), "ContentClaimContainer");
            writeUTFLimited(out, record.getContentClaimSection(), "ContentClaimSection");
            writeUTFLimited(out, record.getContentClaimIdentifier(), "ContentClaimIdentifier");
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
            writeUTFLimited(out, record.getPreviousContentClaimContainer(), "PreviousContentClaimContainer");
            writeUTFLimited(out, record.getPreviousContentClaimSection(), "PreviousContentClaimSection");
            writeUTFLimited(out, record.getPreviousContentClaimIdentifier(), "PreviousContentClaimIdentifier");
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
        writeNullableString(out, record.getSourceQueueIdentifier(), "SourceQueueIdentifier");

        // Write type-specific info
        if (recordType == ProvenanceEventType.FORK || recordType == ProvenanceEventType.JOIN || recordType == ProvenanceEventType.CLONE || recordType == ProvenanceEventType.REPLAY) {
            writeUUIDs(out, record.getParentUuids());
            writeUUIDs(out, record.getChildUuids());
        } else if (recordType == ProvenanceEventType.RECEIVE) {
            writeNullableString(out, record.getTransitUri(), "TransitUri");
            writeNullableString(out, record.getSourceSystemFlowFileIdentifier(), "SourceSystemFlowFileIdentifier");
        } else if (recordType == ProvenanceEventType.FETCH) {
            writeNullableString(out, record.getTransitUri(), "TransitUri");
        } else if (recordType == ProvenanceEventType.SEND) {
            writeNullableString(out, record.getTransitUri(), "TransitUri");
        } else if (recordType == ProvenanceEventType.ADDINFO) {
            writeNullableString(out, record.getAlternateIdentifierUri(), "AlternateIdentifierUri");
        } else if (recordType == ProvenanceEventType.ROUTE) {
            writeNullableString(out, record.getRelationship(), "Relationship");
        }
    }

    protected void writeUUID(final DataOutputStream out, final String uuid) throws IOException {
        writeUTFLimited(out, uuid, "UUID");
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

    protected void writeNullableString(final DataOutputStream out, final String toWrite, String fieldName) throws IOException {
        if (toWrite == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeUTFLimited(out, toWrite, fieldName);
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

    private void writeUTFLimited(final DataOutputStream out, final String utfString, final String fieldName) throws IOException {
        try {
            out.writeUTF(utfString);
        } catch (UTFDataFormatException e) {
            final String truncated = utfString.substring(0, getCharsInUTF8Limit(utfString, MAX_ALLOWED_UTF_LENGTH));
            logger.warn("Truncating repository record value for field '{}'!  Attempted to write {} chars that encode to a UTF8 byte length greater than "
                            + "supported maximum ({}), truncating to {} chars.",
                    (fieldName == null) ? "" : fieldName, utfString.length(), MAX_ALLOWED_UTF_LENGTH, truncated.length());
            if (logger.isDebugEnabled()) {
                logger.warn("String value was:\n{}", truncated);
            }
            out.writeUTF(truncated);
        }
    }

    static int getCharsInUTF8Limit(final String str, final int utf8Limit) {
        // Calculate how much of String fits within UTF8 byte limit based on RFC3629.
        //
        // Java String values use char[] for storage, so character values >0xFFFF that
        // map to 4 byte UTF8 representations are not considered.

        final int charsInOriginal = str.length();
        int bytesInUTF8 = 0;

        for (int i = 0; i < charsInOriginal; i++) {
            final int curr = str.charAt(i);
            if (curr < 0x0080) {
                bytesInUTF8++;
            } else if (curr < 0x0800) {
                bytesInUTF8 += 2;
            } else {
                bytesInUTF8 += 3;
            }
            if (bytesInUTF8 > utf8Limit) {
                return i;
            }
        }
        return charsInOriginal;
    }



    @Override
    public String toString() {
        return "StandardRecordWriter[file=" + file + "]";
    }
}

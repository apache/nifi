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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.nifi.provenance.serialization.CompressableRecordReader;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardRecordReader extends CompressableRecordReader {
    public static final int SERIALIZATION_VERISON = 9;
    public static final String SERIALIZATION_NAME = "org.apache.nifi.provenance.PersistentProvenanceRepository";

    private static final Logger logger = LoggerFactory.getLogger(StandardRecordReader.class);
    private static final Pattern UUID_PATTERN = Pattern.compile("[a-fA-F0-9]{8}\\-([a-fA-F0-9]{4}\\-){3}[a-fA-F0-9]{12}");

    public StandardRecordReader(final InputStream in, final String filename, final int maxAttributeChars) throws IOException {
        this(in, filename, null, maxAttributeChars);
    }

    public StandardRecordReader(final InputStream in, final String filename, final TocReader tocReader, final int maxAttributeChars) throws IOException {
        super(in, filename, tocReader, maxAttributeChars);
        logger.trace("Creating RecordReader for {}", filename);
    }


    private StandardProvenanceEventRecord readPreVersion6Record(final DataInputStream dis, final int serializationVersion) throws IOException {
        final long startOffset = getBytesConsumed();
        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();

        final long eventId = dis.readLong();
        if (serializationVersion == 4) {
            // notion of a UUID for the event was added in Version 4 so that Events can be referred to uniquely
            // across a cluster. This was then removed in version 5 because it was decided that a unique id
            // could better be generated based on the event id and the cluster node identifier.
            // Therefore, we read in the Event Identifier and throw it away.
            dis.readUTF();
        }
        final String eventTypeName = dis.readUTF();
        final ProvenanceEventType eventType = ProvenanceEventType.valueOf(eventTypeName);
        builder.setEventType(eventType);
        builder.setEventTime(dis.readLong());

        if (serializationVersion > 3) {
            // event duration introduced in version 4.
            builder.setEventDuration(dis.readLong());
        }

        dis.readLong(); // Used to persist FlowFileId
        final long fileSize = dis.readLong();

        builder.setComponentId(readNullableString(dis));
        builder.setComponentType(readNullableString(dis));
        builder.setFlowFileUUID(readNullableString(dis));

        final int numParents = dis.readInt();
        for (int i = 0; i < numParents; i++) {
            builder.addParentUuid(dis.readUTF());
        }

        if (serializationVersion > 2) {
            // notion of child UUID's was introduced in version 3.
            final int numChildren = dis.readInt();
            for (int i = 0; i < numChildren; i++) {
                builder.addChildUuid(dis.readUTF());
            }
        }

        final String sourceSystemUri = readNullableString(dis);

        if (serializationVersion > 3) {
            // notion of a source system flowfile identifier was introduced in version 4.
            builder.setSourceSystemFlowFileIdentifier(readNullableString(dis));
        }

        final String destinationSystemUri = readNullableString(dis);
        if (sourceSystemUri != null) {
            builder.setTransitUri(sourceSystemUri);
        } else if (destinationSystemUri != null) {
            builder.setTransitUri(destinationSystemUri);
        }

        readNullableString(dis);    // Content-Type No longer used

        builder.setAlternateIdentifierUri(readNullableString(dis));

        final Map<String, String> attrs = readAttributes(dis, false);

        builder.setFlowFileEntryDate(System.currentTimeMillis());
        builder.setLineageStartDate(-1L);
        builder.setAttributes(Collections.<String, String>emptyMap(), attrs);
        builder.setCurrentContentClaim(null, null, null, null, fileSize);

        builder.setStorageLocation(getFilename(), startOffset);

        final StandardProvenanceEventRecord record = builder.build();
        record.setEventId(eventId);
        return record;
    }

    @Override
    public StandardProvenanceEventRecord nextRecord(final DataInputStream dis, final int serializationVersion) throws IOException {
        if (serializationVersion > SERIALIZATION_VERISON) {
            throw new IllegalArgumentException("Unable to deserialize record because the version is "
                + serializationVersion + " and supported versions are 1-" + SERIALIZATION_VERISON);
        }

        // Schema changed drastically in version 6 so we created a new method to handle old records
        if (serializationVersion < 6) {
            return readPreVersion6Record(dis, serializationVersion);
        }

        final long startOffset = getBytesConsumed();

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();

        final long eventId = dis.readLong();
        final String eventTypeName = dis.readUTF();
        final ProvenanceEventType eventType = ProvenanceEventType.valueOf(eventTypeName);
        builder.setEventType(eventType);
        builder.setEventTime(dis.readLong());

        final Long flowFileEntryDate = dis.readLong();
        builder.setEventDuration(dis.readLong());

        if (serializationVersion < 9){
            final int numLineageIdentifiers = dis.readInt();
            for (int i = 0; i < numLineageIdentifiers; i++) {
                readUUID(dis, serializationVersion); //skip identifiers
            }
        }

        final long lineageStartDate = dis.readLong();

        final long fileSize;
        if (serializationVersion < 7) {
            fileSize = dis.readLong();  // file size moved in version 7 to be with content claims
            builder.setCurrentContentClaim(null, null, null, null, fileSize);
        }

        builder.setComponentId(readNullableString(dis));
        builder.setComponentType(readNullableString(dis));

        final String uuid = readUUID(dis, serializationVersion);
        builder.setFlowFileUUID(uuid);
        builder.setDetails(readNullableString(dis));

        // Read in the FlowFile Attributes
        if (serializationVersion >= 7) {
            final Map<String, String> previousAttrs = readAttributes(dis, false);
            final Map<String, String> attrUpdates = readAttributes(dis, true);
            builder.setAttributes(previousAttrs, attrUpdates);

            final boolean hasContentClaim = dis.readBoolean();
            if (hasContentClaim) {
                builder.setCurrentContentClaim(dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readLong(), dis.readLong());
            } else {
                builder.setCurrentContentClaim(null, null, null, null, 0L);
            }

            final boolean hasPreviousClaim = dis.readBoolean();
            if (hasPreviousClaim) {
                builder.setPreviousContentClaim(dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readLong(), dis.readLong());
            }

            builder.setSourceQueueIdentifier(readNullableString(dis));
        } else {
            final Map<String, String> attrs = readAttributes(dis, false);
            builder.setAttributes(Collections.<String, String>emptyMap(), attrs);
        }

        // Read Event-Type specific fields.
        if (eventType == ProvenanceEventType.FORK || eventType == ProvenanceEventType.JOIN || eventType == ProvenanceEventType.CLONE || eventType == ProvenanceEventType.REPLAY) {
            final int numParents = dis.readInt();
            for (int i = 0; i < numParents; i++) {
                builder.addParentUuid(readUUID(dis, serializationVersion));
            }

            final int numChildren = dis.readInt();
            for (int i = 0; i < numChildren; i++) {
                builder.addChildUuid(readUUID(dis, serializationVersion));
            }
        } else if (eventType == ProvenanceEventType.RECEIVE) {
            builder.setTransitUri(readNullableString(dis));
            builder.setSourceSystemFlowFileIdentifier(readNullableString(dis));
        } else if (eventType == ProvenanceEventType.FETCH) {
            builder.setTransitUri(readNullableString(dis));
        } else if (eventType == ProvenanceEventType.SEND) {
            builder.setTransitUri(readNullableString(dis));
        } else if (eventType == ProvenanceEventType.ADDINFO) {
            builder.setAlternateIdentifierUri(readNullableString(dis));
        } else if (eventType == ProvenanceEventType.ROUTE) {
            builder.setRelationship(readNullableString(dis));
        }

        builder.setFlowFileEntryDate(flowFileEntryDate);
        builder.setLineageStartDate(lineageStartDate);
        builder.setStorageLocation(getFilename(), startOffset);

        final StandardProvenanceEventRecord record = builder.build();
        record.setEventId(eventId);
        return record;
    }

    private Map<String, String> readAttributes(final DataInputStream dis, final boolean valueNullable) throws IOException {
        final int numAttributes = dis.readInt();
        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < numAttributes; i++) {
            final String key = readLongString(dis);
            final String value = valueNullable ? readLongNullableString(dis) : readLongString(dis);
            final String truncatedValue;
            if (value == null) {
                truncatedValue = null;
            } else if (value.length() > getMaxAttributeLength()) {
                truncatedValue = value.substring(0, getMaxAttributeLength());
            } else {
                truncatedValue = value;
            }

            attrs.put(key, truncatedValue);
        }

        return attrs;
    }

    private String readUUID(final DataInputStream in, final int serializationVersion) throws IOException {
        if (serializationVersion < 8) {
            final long msb = in.readLong();
            final long lsb = in.readLong();
            return new UUID(msb, lsb).toString();
        } else {
            // before version 8, we serialized UUID's as two longs in order to
            // write less data. However, in version 8 we changed to just writing
            // out the string because it's extremely expensive to call UUID.fromString.
            // In the end, since we generally compress, the savings in minimal anyway.
            final String uuid = in.readUTF();
            if (!UUID_PATTERN.matcher(uuid).matches()) {
                throw new IOException("Failed to parse Provenance Event Record: expected a UUID but got: " + uuid);
            }
            return uuid;
        }
    }

    private String readNullableString(final DataInputStream in) throws IOException {
        final boolean valueExists = in.readBoolean();
        if (valueExists) {
            return in.readUTF();
        } else {
            return null;
        }
    }

    private String readLongNullableString(final DataInputStream in) throws IOException {
        final boolean valueExists = in.readBoolean();
        if (valueExists) {
            return readLongString(in);
        } else {
            return null;
        }
    }

    private String readLongString(final DataInputStream in) throws IOException {
        final int length = in.readInt();
        final byte[] strBytes = new byte[length];
        StreamUtils.fillBuffer(in, strBytes);
        return new String(strBytes, "UTF-8");
    }
}

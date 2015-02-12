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
package org.apache.nifi.provenance.journaling.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.IdEnrichedProvenanceEvent;
import org.apache.nifi.stream.io.StreamUtils;

public class StandardEventDeserializer implements Deserializer {
    public static final String CODEC_NAME = StandardEventSerializer.CODEC_NAME;
    
    @Override
    public String getCodecName() {
        return CODEC_NAME;
    }
    
    @Override
    public ProvenanceEventRecord deserialize(final DataInputStream in, final int serializationVersion) throws IOException {
        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();

        final long eventId = in.readLong();
        final String eventTypeName = in.readUTF();
        final ProvenanceEventType eventType = ProvenanceEventType.valueOf(eventTypeName);
        builder.setEventType(eventType);
        builder.setEventTime(in.readLong());

        final Long flowFileEntryDate = in.readLong();
        builder.setEventDuration(in.readLong());

        final Set<String> lineageIdentifiers = new HashSet<>();
        final int numLineageIdentifiers = in.readInt();
        for (int i = 0; i < numLineageIdentifiers; i++) {
            lineageIdentifiers.add(readUUID(in));
        }

        final long lineageStartDate = in.readLong();

        builder.setComponentId(readNullableString(in));
        builder.setComponentType(readNullableString(in));

        final String uuid = readUUID(in);
        builder.setFlowFileUUID(uuid);
        builder.setDetails(readNullableString(in));

        // Read in the FlowFile Attributes
        final Map<String, String> previousAttrs = readAttributes(in, false);
        final Map<String, String> attrUpdates = readAttributes(in, true);
        builder.setAttributes(previousAttrs, attrUpdates);

        final boolean hasContentClaim = in.readBoolean();
        if (hasContentClaim) {
            builder.setCurrentContentClaim(in.readUTF(), in.readUTF(), in.readUTF(), in.readLong(), in.readLong());
        } else {
            builder.setCurrentContentClaim(null, null, null, null, 0L);
        }

        final boolean hasPreviousClaim = in.readBoolean();
        if (hasPreviousClaim) {
            builder.setPreviousContentClaim(in.readUTF(), in.readUTF(), in.readUTF(), in.readLong(), in.readLong());
        }

        builder.setSourceQueueIdentifier(readNullableString(in));

        // Read Event-Type specific fields.
        if (eventType == ProvenanceEventType.FORK || eventType == ProvenanceEventType.JOIN || eventType == ProvenanceEventType.CLONE || eventType == ProvenanceEventType.REPLAY) {
            final int numParents = in.readInt();
            for (int i = 0; i < numParents; i++) {
                builder.addParentUuid(readUUID(in));
            }

            final int numChildren = in.readInt();
            for (int i = 0; i < numChildren; i++) {
                builder.addChildUuid(readUUID(in));
            }
        } else if (eventType == ProvenanceEventType.RECEIVE) {
            builder.setTransitUri(readNullableString(in));
            builder.setSourceSystemFlowFileIdentifier(readNullableString(in));
        } else if (eventType == ProvenanceEventType.SEND) {
            builder.setTransitUri(readNullableString(in));
        } else if (eventType == ProvenanceEventType.ADDINFO) {
            builder.setAlternateIdentifierUri(readNullableString(in));
        } else if (eventType == ProvenanceEventType.ROUTE) {
            builder.setRelationship(readNullableString(in));
        }

        builder.setFlowFileEntryDate(flowFileEntryDate);
        builder.setLineageIdentifiers(lineageIdentifiers);
        builder.setLineageStartDate(lineageStartDate);
        final ProvenanceEventRecord event = builder.build();
        
        return new IdEnrichedProvenanceEvent(event, eventId);
    }

    
    private static Map<String, String> readAttributes(final DataInputStream dis, final boolean valueNullable) throws IOException {
        final int numAttributes = dis.readInt();
        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < numAttributes; i++) {
            final String key = readLongString(dis);
            final String value = valueNullable ? readLongNullableString(dis) : readLongString(dis);
            attrs.put(key, value);
        }

        return attrs;
    }

    private static String readUUID(final DataInputStream in) throws IOException {
        final long msb = in.readLong();
        final long lsb = in.readLong();
        return new UUID(msb, lsb).toString();
    }

    private static String readNullableString(final DataInputStream in) throws IOException {
        final boolean valueExists = in.readBoolean();
        if (valueExists) {
            return in.readUTF();
        } else {
            return null;
        }
    }

    private static String readLongNullableString(final DataInputStream in) throws IOException {
        final boolean valueExists = in.readBoolean();
        if (valueExists) {
            return readLongString(in);
        } else {
            return null;
        }
    }

    private static String readLongString(final DataInputStream in) throws IOException {
        final int length = in.readInt();
        final byte[] strBytes = new byte[length];
        StreamUtils.fillBuffer(in, strBytes);
        return new String(strBytes, StandardCharsets.UTF_8);
    }
}

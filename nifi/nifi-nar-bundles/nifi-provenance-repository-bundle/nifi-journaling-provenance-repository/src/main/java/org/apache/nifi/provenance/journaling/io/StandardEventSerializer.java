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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

public class StandardEventSerializer implements Serializer {
    public static final String CODEC_NAME = "StandardProvCodec";
    
    @Override
    public int getVersion() {
        return 1;
    }
    
    @Override
    public String getCodecName() {
        return CODEC_NAME;
    }
    
    @Override
    public void serialize(final ProvenanceEventRecord event, final DataOutputStream out) throws IOException {
        final ProvenanceEventType recordType = event.getEventType();

        out.writeUTF(event.getEventType().name());
        out.writeLong(event.getEventTime());
        out.writeLong(event.getFlowFileEntryDate());
        out.writeLong(event.getEventDuration());

        writeUUIDs(out, event.getLineageIdentifiers());
        out.writeLong(event.getLineageStartDate());

        writeNullableString(out, event.getComponentId());
        writeNullableString(out, event.getComponentType());
        writeUUID(out, event.getFlowFileUuid());
        writeNullableString(out, event.getDetails());

        // Write FlowFile attributes
        final Map<String, String> attrs = event.getPreviousAttributes();
        out.writeInt(attrs.size());
        for (final Map.Entry<String, String> entry : attrs.entrySet()) {
            writeLongString(out, entry.getKey());
            writeLongString(out, entry.getValue());
        }

        final Map<String, String> attrUpdates = event.getUpdatedAttributes();
        out.writeInt(attrUpdates.size());
        for (final Map.Entry<String, String> entry : attrUpdates.entrySet()) {
            writeLongString(out, entry.getKey());
            writeLongNullableString(out, entry.getValue());
        }

        // If Content Claim Info is present, write out a 'TRUE' followed by claim info. Else, write out 'false'. 
        if (event.getContentClaimSection() != null && event.getContentClaimContainer() != null && event.getContentClaimIdentifier() != null) {
            out.writeBoolean(true);
            out.writeUTF(event.getContentClaimContainer());
            out.writeUTF(event.getContentClaimSection());
            out.writeUTF(event.getContentClaimIdentifier());
            if (event.getContentClaimOffset() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(event.getContentClaimOffset());
            }
            out.writeLong(event.getFileSize());
        } else {
            out.writeBoolean(false);
        }

        // If Previous Content Claim Info is present, write out a 'TRUE' followed by claim info. Else, write out 'false'.
        if (event.getPreviousContentClaimSection() != null && event.getPreviousContentClaimContainer() != null && event.getPreviousContentClaimIdentifier() != null) {
            out.writeBoolean(true);
            out.writeUTF(event.getPreviousContentClaimContainer());
            out.writeUTF(event.getPreviousContentClaimSection());
            out.writeUTF(event.getPreviousContentClaimIdentifier());
            if (event.getPreviousContentClaimOffset() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(event.getPreviousContentClaimOffset());
            }

            if (event.getPreviousFileSize() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(event.getPreviousFileSize());
            }
        } else {
            out.writeBoolean(false);
        }

        // write out the identifier of the destination queue.
        writeNullableString(out, event.getSourceQueueIdentifier());

        // Write type-specific info
        if (recordType == ProvenanceEventType.FORK || recordType == ProvenanceEventType.JOIN || recordType == ProvenanceEventType.CLONE || recordType == ProvenanceEventType.REPLAY) {
            writeUUIDs(out, event.getParentUuids());
            writeUUIDs(out, event.getChildUuids());
        } else if (recordType == ProvenanceEventType.RECEIVE) {
            writeNullableString(out, event.getTransitUri());
            writeNullableString(out, event.getSourceSystemFlowFileIdentifier());
        } else if (recordType == ProvenanceEventType.SEND) {
            writeNullableString(out, event.getTransitUri());
        } else if (recordType == ProvenanceEventType.ADDINFO) {
            writeNullableString(out, event.getAlternateIdentifierUri());
        } else if (recordType == ProvenanceEventType.ROUTE) {
            writeNullableString(out, event.getRelationship());
        }
    }

    private void writeNullableString(final DataOutputStream out, final String toWrite) throws IOException {
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
    
    static void writeUUID(final DataOutputStream out, final String uuid) throws IOException {
        final UUID uuidObj = UUID.fromString(uuid);
        out.writeLong(uuidObj.getMostSignificantBits());
        out.writeLong(uuidObj.getLeastSignificantBits());
    }

    static void writeUUIDs(final DataOutputStream out, final Collection<String> list) throws IOException {
        if (list == null) {
            out.writeInt(0);
        } else {
            out.writeInt(list.size());
            for (final String value : list) {
                writeUUID(out, value);
            }
        }
    }

}

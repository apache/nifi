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

package org.apache.nifi.controller.repository;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDe;
import org.wali.UpdateType;

public class WriteAheadRepositoryRecordSerde extends RepositoryRecordSerde implements SerDe<RepositoryRecord> {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadRepositoryRecordSerde.class);

    private static final int CURRENT_ENCODING_VERSION = 9;

    public static final byte ACTION_CREATE = 0;
    public static final byte ACTION_UPDATE = 1;
    public static final byte ACTION_DELETE = 2;
    public static final byte ACTION_SWAPPED_OUT = 3;
    public static final byte ACTION_SWAPPED_IN = 4;

    private long recordsRestored = 0L;
    private final ResourceClaimManager claimManager;

    public WriteAheadRepositoryRecordSerde(final ResourceClaimManager claimManager) {
        this.claimManager = claimManager;
    }

    @Override
    public void serializeEdit(final RepositoryRecord previousRecordState, final RepositoryRecord record, final DataOutputStream out) throws IOException {
        serializeEdit(previousRecordState, record, out, false);
    }

    public void serializeEdit(final RepositoryRecord previousRecordState, final RepositoryRecord record, final DataOutputStream out, final boolean forceAttributesWritten) throws IOException {
        if (record.isMarkedForAbort()) {
            logger.warn("Repository Record {} is marked to be aborted; it will be persisted in the FlowFileRepository as a DELETE record", record);
            out.write(ACTION_DELETE);
            out.writeLong(getRecordIdentifier(record));
            serializeContentClaim(record.getCurrentClaim(), record.getCurrentClaimOffset(), out);
            return;
        }

        final UpdateType updateType = getUpdateType(record);

        if (updateType.equals(UpdateType.DELETE)) {
            out.write(ACTION_DELETE);
            out.writeLong(getRecordIdentifier(record));
            serializeContentClaim(record.getCurrentClaim(), record.getCurrentClaimOffset(), out);
            return;
        }

        // If there's a Destination Connection, that's the one that we want to associated with this record.
        // However, on restart, we will restore the FlowFile and set this connection to its "originalConnection".
        // If we then serialize the FlowFile again before it's transferred, it's important to allow this to happen,
        // so we use the originalConnection instead
        FlowFileQueue associatedQueue = record.getDestination();
        if (associatedQueue == null) {
            associatedQueue = record.getOriginalQueue();
        }

        if (updateType.equals(UpdateType.SWAP_OUT)) {
            out.write(ACTION_SWAPPED_OUT);
            out.writeLong(getRecordIdentifier(record));
            out.writeUTF(associatedQueue.getIdentifier());
            out.writeUTF(getLocation(record));
            return;
        }

        final FlowFile flowFile = record.getCurrent();
        final ContentClaim claim = record.getCurrentClaim();

        switch (updateType) {
            case UPDATE:
                out.write(ACTION_UPDATE);
                break;
            case CREATE:
                out.write(ACTION_CREATE);
                break;
            case SWAP_IN:
                out.write(ACTION_SWAPPED_IN);
                break;
            default:
                throw new AssertionError();
        }

        out.writeLong(getRecordIdentifier(record));
        out.writeLong(flowFile.getEntryDate());
        out.writeLong(flowFile.getLineageStartDate());
        out.writeLong(flowFile.getLineageStartIndex());

        final Long queueDate = flowFile.getLastQueueDate();
        out.writeLong(queueDate == null ? System.currentTimeMillis() : queueDate);
        out.writeLong(flowFile.getQueueDateIndex());
        out.writeLong(flowFile.getSize());

        if (associatedQueue == null) {
            logger.warn("{} Repository Record {} has no Connection associated with it; it will be destroyed on restart",
                new Object[] {this, record});
            writeString("", out);
        } else {
            writeString(associatedQueue.getIdentifier(), out);
        }

        serializeContentClaim(claim, record.getCurrentClaimOffset(), out);

        if (forceAttributesWritten || record.isAttributesChanged() || updateType == UpdateType.CREATE || updateType == UpdateType.SWAP_IN) {
            out.write(1);   // indicate attributes changed
            final Map<String, String> attributes = flowFile.getAttributes();
            out.writeInt(attributes.size());
            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                writeString(entry.getKey(), out);
                writeString(entry.getValue(), out);
            }
        } else {
            out.write(0);   // indicate attributes did not change
        }

        if (updateType == UpdateType.SWAP_IN) {
            out.writeUTF(record.getSwapLocation());
        }
    }

    @Override
    public RepositoryRecord deserializeEdit(final DataInputStream in, final Map<Object, RepositoryRecord> currentRecordStates, final int version) throws IOException {
        final int action = in.read();
        final long recordId = in.readLong();
        if (action == ACTION_DELETE) {
            final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder().id(recordId);

            if (version > 4) {
                deserializeClaim(in, version, ffBuilder);
            }

            final FlowFileRecord flowFileRecord = ffBuilder.build();
            final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null, flowFileRecord);
            record.markForDelete();

            return record;
        }

        if (action == ACTION_SWAPPED_OUT) {
            final String queueId = in.readUTF();
            final String location = in.readUTF();
            final FlowFileQueue queue = getFlowFileQueue(queueId);

            final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .id(recordId)
                .build();

            return new StandardRepositoryRecord(queue, flowFileRecord, location);
        }

        final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        final RepositoryRecord record = currentRecordStates.get(recordId);
        ffBuilder.id(recordId);
        if (record != null) {
            ffBuilder.fromFlowFile(record.getCurrent());
        }
        ffBuilder.entryDate(in.readLong());

        if (version > 1) {
            // read the lineage identifiers and lineage start date, which were added in version 2.
            if (version < 9) {
                final int numLineageIds = in.readInt();
                for (int i = 0; i < numLineageIds; i++) {
                    in.readUTF(); //skip identifiers
                }
            }
            final long lineageStartDate = in.readLong();
            final long lineageStartIndex;
            if (version > 7) {
                lineageStartIndex = in.readLong();
            } else {
                lineageStartIndex = 0L;
            }
            ffBuilder.lineageStart(lineageStartDate, lineageStartIndex);

            if (version > 5) {
                final long lastQueueDate = in.readLong();
                final long queueDateIndex;
                if (version > 7) {
                    queueDateIndex = in.readLong();
                } else {
                    queueDateIndex = 0L;
                }

                ffBuilder.lastQueued(lastQueueDate, queueDateIndex);
            }
        }

        ffBuilder.size(in.readLong());
        final String connectionId = readString(in);

        logger.debug("{} -> {}", new Object[] {recordId, connectionId});

        deserializeClaim(in, version, ffBuilder);

        // recover new attributes, if they changed
        final int attributesChanged = in.read();
        if (attributesChanged == -1) {
            throw new EOFException();
        } else if (attributesChanged == 1) {
            final int numAttributes = in.readInt();
            final Map<String, String> attributes = new HashMap<>();
            for (int i = 0; i < numAttributes; i++) {
                final String key = readString(in);
                final String value = readString(in);
                attributes.put(key, value);
            }

            ffBuilder.addAttributes(attributes);
        } else if (attributesChanged != 0) {
            throw new IOException("Attribute Change Qualifier not found in stream; found value: "
                + attributesChanged + " after successfully restoring " + recordsRestored + " records. The FlowFile Repository appears to be corrupt!");
        }

        final FlowFileRecord flowFile = ffBuilder.build();
        String swapLocation = null;
        if (action == ACTION_SWAPPED_IN) {
            swapLocation = in.readUTF();
        }

        final FlowFileQueue queue = getFlowFileQueue(connectionId);
        final StandardRepositoryRecord standardRepoRecord = new StandardRepositoryRecord(queue, flowFile);
        if (swapLocation != null) {
            standardRepoRecord.setSwapLocation(swapLocation);
        }

        if (connectionId.isEmpty()) {
            logger.warn("{} does not have a Queue associated with it; this record will be discarded", flowFile);
            standardRepoRecord.markForAbort();
        } else if (queue == null) {
            logger.warn("{} maps to unknown Queue {}; this record will be discarded", flowFile, connectionId);
            standardRepoRecord.markForAbort();
        }

        recordsRestored++;
        return standardRepoRecord;
    }

    @Override
    public StandardRepositoryRecord deserializeRecord(final DataInputStream in, final int version) throws IOException {
        final int action = in.read();
        if (action == -1) {
            return null;
        }

        final long recordId = in.readLong();
        if (action == ACTION_DELETE) {
            final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder().id(recordId);

            if (version > 4) {
                deserializeClaim(in, version, ffBuilder);
            }

            final FlowFileRecord flowFileRecord = ffBuilder.build();
            final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null, flowFileRecord);
            record.markForDelete();
            return record;
        }

        // if action was not delete, it must be create/swap in
        final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        final long entryDate = in.readLong();

        if (version > 1) {
            // read the lineage identifiers and lineage start date, which were added in version 2.
            if (version < 9) {
                final int numLineageIds = in.readInt();
                for (int i = 0; i < numLineageIds; i++) {
                    in.readUTF(); //skip identifiers
                }
            }

            final long lineageStartDate = in.readLong();
            final long lineageStartIndex;
            if (version > 7) {
                lineageStartIndex = in.readLong();
            } else {
                lineageStartIndex = 0L;
            }
            ffBuilder.lineageStart(lineageStartDate, lineageStartIndex);

            if (version > 5) {
                final long lastQueueDate = in.readLong();
                final long queueDateIndex;
                if (version > 7) {
                    queueDateIndex = in.readLong();
                } else {
                    queueDateIndex = 0L;
                }

                ffBuilder.lastQueued(lastQueueDate, queueDateIndex);
            }
        }

        final long size = in.readLong();
        final String connectionId = readString(in);

        logger.debug("{} -> {}", new Object[] {recordId, connectionId});

        ffBuilder.id(recordId);
        ffBuilder.entryDate(entryDate);
        ffBuilder.size(size);

        deserializeClaim(in, version, ffBuilder);

        final int attributesChanged = in.read();
        if (attributesChanged == 1) {
            final int numAttributes = in.readInt();
            final Map<String, String> attributes = new HashMap<>();
            for (int i = 0; i < numAttributes; i++) {
                final String key = readString(in);
                final String value = readString(in);
                attributes.put(key, value);
            }

            ffBuilder.addAttributes(attributes);
        } else if (attributesChanged == -1) {
            throw new EOFException();
        } else if (attributesChanged != 0) {
            throw new IOException("Attribute Change Qualifier not found in stream; found value: "
                + attributesChanged + " after successfully restoring " + recordsRestored + " records");
        }

        final FlowFileRecord flowFile = ffBuilder.build();
        String swapLocation = null;
        if (action == ACTION_SWAPPED_IN) {
            swapLocation = in.readUTF();
        }

        final StandardRepositoryRecord record;
        final FlowFileQueue queue = getFlowFileQueue(connectionId);
        record = new StandardRepositoryRecord(queue, flowFile);
        if (swapLocation != null) {
            record.setSwapLocation(swapLocation);
        }

        if (connectionId.isEmpty()) {
            logger.warn("{} does not have a FlowFile Queue associated with it; this record will be discarded", flowFile);
            record.markForAbort();
        } else if (queue == null) {
            logger.warn("{} maps to unknown FlowFile Queue {}; this record will be discarded", flowFile, connectionId);
            record.markForAbort();
        }

        recordsRestored++;
        return record;
    }

    @Override
    public void serializeRecord(final RepositoryRecord record, final DataOutputStream out) throws IOException {
        serializeEdit(null, record, out, true);
    }

    private void serializeContentClaim(final ContentClaim claim, final long offset, final DataOutputStream out) throws IOException {
        if (claim == null) {
            out.write(0);
        } else {
            out.write(1);

            final ResourceClaim resourceClaim = claim.getResourceClaim();
            writeString(resourceClaim.getId(), out);
            writeString(resourceClaim.getContainer(), out);
            writeString(resourceClaim.getSection(), out);
            out.writeLong(claim.getOffset());
            out.writeLong(claim.getLength());

            out.writeLong(offset);
            out.writeBoolean(resourceClaim.isLossTolerant());
        }
    }

    private void deserializeClaim(final DataInputStream in, final int serializationVersion, final StandardFlowFileRecord.Builder ffBuilder) throws IOException {
        // determine current Content Claim.
        final int claimExists = in.read();
        if (claimExists == 1) {
            final String claimId;
            if (serializationVersion < 4) {
                claimId = String.valueOf(in.readLong());
            } else {
                claimId = readString(in);
            }

            final String container = readString(in);
            final String section = readString(in);

            final long resourceOffset;
            final long resourceLength;
            if (serializationVersion < 7) {
                resourceOffset = 0L;
                resourceLength = -1L;
            } else {
                resourceOffset = in.readLong();
                resourceLength = in.readLong();
            }

            final long claimOffset = in.readLong();

            final boolean lossTolerant;
            if (serializationVersion >= 3) {
                lossTolerant = in.readBoolean();
            } else {
                lossTolerant = false;
            }

            final ResourceClaim resourceClaim = claimManager.newResourceClaim(container, section, claimId, lossTolerant, false);
            final StandardContentClaim contentClaim = new StandardContentClaim(resourceClaim, resourceOffset);
            contentClaim.setLength(resourceLength);

            ffBuilder.contentClaim(contentClaim);
            ffBuilder.contentClaimOffset(claimOffset);
        } else if (claimExists == -1) {
            throw new EOFException();
        } else if (claimExists != 0) {
            throw new IOException("Claim Existence Qualifier not found in stream; found value: "
                + claimExists + " after successfully restoring " + recordsRestored + " records");
        }
    }

    private void writeString(final String toWrite, final OutputStream out) throws IOException {
        final byte[] bytes = toWrite.getBytes("UTF-8");
        final int utflen = bytes.length;

        if (utflen < 65535) {
            out.write(utflen >>> 8);
            out.write(utflen);
            out.write(bytes);
        } else {
            out.write(255);
            out.write(255);
            out.write(utflen >>> 24);
            out.write(utflen >>> 16);
            out.write(utflen >>> 8);
            out.write(utflen);
            out.write(bytes);
        }
    }

    private String readString(final InputStream in) throws IOException {
        final Integer numBytes = readFieldLength(in);
        if (numBytes == null) {
            throw new EOFException();
        }
        final byte[] bytes = new byte[numBytes];
        fillBuffer(in, bytes, numBytes);
        return new String(bytes, "UTF-8");
    }

    private Integer readFieldLength(final InputStream in) throws IOException {
        final int firstValue = in.read();
        final int secondValue = in.read();
        if (firstValue < 0) {
            return null;
        }
        if (secondValue < 0) {
            throw new EOFException();
        }
        if (firstValue == 0xff && secondValue == 0xff) {
            final int ch1 = in.read();
            final int ch2 = in.read();
            final int ch3 = in.read();
            final int ch4 = in.read();
            if ((ch1 | ch2 | ch3 | ch4) < 0) {
                throw new EOFException();
            }
            return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4;
        } else {
            return (firstValue << 8) + secondValue;
        }
    }

    private void fillBuffer(final InputStream in, final byte[] buffer, final int length) throws IOException {
        int bytesRead;
        int totalBytesRead = 0;
        while ((bytesRead = in.read(buffer, totalBytesRead, length - totalBytesRead)) > 0) {
            totalBytesRead += bytesRead;
        }
        if (totalBytesRead != length) {
            throw new EOFException();
        }
    }

    @Override
    public int getVersion() {
        return CURRENT_ENCODING_VERSION;
    }
}
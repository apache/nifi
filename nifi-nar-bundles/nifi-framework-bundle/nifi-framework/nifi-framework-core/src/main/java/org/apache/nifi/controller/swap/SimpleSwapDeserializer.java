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

package org.apache.nifi.controller.swap;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.IncompleteSwapFileException;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSwapDeserializer implements SwapDeserializer {
    public static final int SWAP_ENCODING_VERSION = 10;
    private static final Logger logger = LoggerFactory.getLogger(SimpleSwapDeserializer.class);

    @Override
    public SwapSummary getSwapSummary(final DataInputStream in, final String swapLocation, final ResourceClaimManager claimManager) throws IOException {
        final int swapEncodingVersion = in.readInt();
        if (swapEncodingVersion > SWAP_ENCODING_VERSION) {
            final String errMsg = "Cannot swap FlowFiles in from " + swapLocation + " because the encoding version is "
                + swapEncodingVersion + ", which is too new (expecting " + SWAP_ENCODING_VERSION + " or less)";

            throw new IOException(errMsg);
        }

        final int numRecords;
        final long contentSize;
        Long maxRecordId = null;
        try {
            in.readUTF(); // ignore Connection ID
            numRecords = in.readInt();
            contentSize = in.readLong();

            if (numRecords == 0) {
                return StandardSwapSummary.EMPTY_SUMMARY;
            }

            if (swapEncodingVersion > 7) {
                maxRecordId = in.readLong();
            }
        } catch (final EOFException eof) {
            logger.warn("Found premature End-of-File when reading Swap File {}. EOF occurred before any FlowFiles were encountered", swapLocation);
            return StandardSwapSummary.EMPTY_SUMMARY;
        }

        final QueueSize queueSize = new QueueSize(numRecords, contentSize);
        final SwapContents swapContents = deserializeFlowFiles(in, queueSize, maxRecordId, swapEncodingVersion, claimManager, swapLocation);
        return swapContents.getSummary();
    }


    @Override
    public SwapContents deserializeFlowFiles(final DataInputStream in, final String swapLocation, final FlowFileQueue queue, final ResourceClaimManager claimManager) throws IOException {
        final int swapEncodingVersion = in.readInt();
        if (swapEncodingVersion > SWAP_ENCODING_VERSION) {
            throw new IOException("Cannot swap FlowFiles in from SwapFile because the encoding version is "
                + swapEncodingVersion + ", which is too new (expecting " + SWAP_ENCODING_VERSION + " or less)");
        }

        final String connectionId = in.readUTF(); // Connection ID
        if (!connectionId.equals(queue.getIdentifier())) {
            throw new IllegalArgumentException("Cannot deserialize FlowFiles from Swap File at location " + swapLocation
                + " because those FlowFiles belong to Connection with ID " + connectionId + " and an attempt was made to swap them into a Connection with ID " + queue.getIdentifier());
        }

        int numRecords = 0;
        long contentSize = 0L;
        Long maxRecordId = null;
        try {
            numRecords = in.readInt();
            contentSize = in.readLong(); // Content Size
            if (swapEncodingVersion > 7) {
                maxRecordId = in.readLong(); // Max Record ID
            }
        } catch (final EOFException eof) {
            final QueueSize queueSize = new QueueSize(numRecords, contentSize);
            final SwapSummary summary = new StandardSwapSummary(queueSize, maxRecordId, Collections.emptyList());
            final SwapContents partialContents = new StandardSwapContents(summary, Collections.emptyList());
            throw new IncompleteSwapFileException(swapLocation, partialContents);
        }

        final QueueSize queueSize = new QueueSize(numRecords, contentSize);
        return deserializeFlowFiles(in, queueSize, maxRecordId, swapEncodingVersion, claimManager, swapLocation);
    }

    private static SwapContents deserializeFlowFiles(final DataInputStream in, final QueueSize queueSize, final Long maxRecordId,
        final int serializationVersion, final ResourceClaimManager claimManager, final String location) throws IOException {
        final List<FlowFileRecord> flowFiles = new ArrayList<>(queueSize.getObjectCount());
        final List<ResourceClaim> resourceClaims = new ArrayList<>(queueSize.getObjectCount());
        Long maxId = maxRecordId;

        for (int i = 0; i < queueSize.getObjectCount(); i++) {
            try {
                // legacy encoding had an "action" because it used to be couple with FlowFile Repository code
                if (serializationVersion < 3) {
                    final int action = in.read();
                    if (action != 1) {
                        throw new IOException("Swap File is version " + serializationVersion + " but did not contain a 'UPDATE' record type");
                    }
                }

                final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
                final long recordId = in.readLong();
                if (maxId == null || recordId > maxId) {
                    maxId = recordId;
                }

                ffBuilder.id(recordId);
                ffBuilder.entryDate(in.readLong());

                if (serializationVersion > 1) {
                    // Lineage information was added in version 2
                    if (serializationVersion < 10) {
                        final int numLineageIdentifiers = in.readInt();
                        for (int lineageIdIdx = 0; lineageIdIdx < numLineageIdentifiers; lineageIdIdx++) {
                            in.readUTF(); //skip each identifier
                        }
                    }

                    // version 9 adds in a 'lineage start index'
                    final long lineageStartDate = in.readLong();
                    final long lineageStartIndex;
                    if (serializationVersion > 8) {
                        lineageStartIndex = in.readLong();
                    } else {
                        lineageStartIndex = 0L;
                    }

                    ffBuilder.lineageStart(lineageStartDate, lineageStartIndex);

                    if (serializationVersion > 5) {
                        // Version 9 adds in a 'queue date index'
                        final long lastQueueDate = in.readLong();
                        final long queueDateIndex;
                        if (serializationVersion > 8) {
                            queueDateIndex = in.readLong();
                        } else {
                            queueDateIndex = 0L;
                        }

                        ffBuilder.lastQueued(lastQueueDate, queueDateIndex);
                    }
                }

                ffBuilder.size(in.readLong());

                if (serializationVersion < 3) {
                    readString(in); // connection Id
                }

                final boolean hasClaim = in.readBoolean();
                ResourceClaim resourceClaim = null;
                if (hasClaim) {
                    final String claimId;
                    if (serializationVersion < 5) {
                        claimId = String.valueOf(in.readLong());
                    } else {
                        claimId = in.readUTF();
                    }

                    final String container = in.readUTF();
                    final String section = in.readUTF();

                    final long resourceOffset;
                    final long resourceLength;
                    if (serializationVersion < 6) {
                        resourceOffset = 0L;
                        resourceLength = -1L;
                    } else {
                        resourceOffset = in.readLong();
                        resourceLength = in.readLong();
                    }

                    final long claimOffset = in.readLong();

                    final boolean lossTolerant;
                    if (serializationVersion >= 4) {
                        lossTolerant = in.readBoolean();
                    } else {
                        lossTolerant = false;
                    }

                    resourceClaim = claimManager.getResourceClaim(container, section, claimId);
                    if (resourceClaim == null) {
                        logger.error("Swap file indicates that FlowFile was referencing Resource Claim at container={}, section={}, claimId={}, "
                            + "but this Resource Claim cannot be found! Will create a temporary Resource Claim, but this may affect the framework's "
                            + "ability to properly clean up this resource", container, section, claimId);
                        resourceClaim = claimManager.newResourceClaim(container, section, claimId, lossTolerant, true);
                    }

                    final StandardContentClaim claim = new StandardContentClaim(resourceClaim, resourceOffset);
                    claim.setLength(resourceLength);

                    ffBuilder.contentClaim(claim);
                    ffBuilder.contentClaimOffset(claimOffset);
                }

                boolean attributesChanged = true;
                if (serializationVersion < 3) {
                    attributesChanged = in.readBoolean();
                }

                if (attributesChanged) {
                    final int numAttributes = in.readInt();
                    for (int j = 0; j < numAttributes; j++) {
                        final String key = readString(in);
                        final String value = readString(in);

                        ffBuilder.addAttribute(key, value);
                    }
                }

                final FlowFileRecord record = ffBuilder.build();
                if (resourceClaim != null) {
                    resourceClaims.add(resourceClaim);
                }

                flowFiles.add(record);
            } catch (final EOFException eof) {
                final SwapSummary swapSummary = new StandardSwapSummary(queueSize, maxId, resourceClaims);
                final SwapContents partialContents = new StandardSwapContents(swapSummary, flowFiles);
                throw new IncompleteSwapFileException(location, partialContents);
            }
        }

        final SwapSummary swapSummary = new StandardSwapSummary(queueSize, maxId, resourceClaims);
        return new StandardSwapContents(swapSummary, flowFiles);
    }

    private static String readString(final InputStream in) throws IOException {
        final Integer numBytes = readFieldLength(in);
        if (numBytes == null) {
            throw new EOFException();
        }
        final byte[] bytes = new byte[numBytes];
        fillBuffer(in, bytes, numBytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Integer readFieldLength(final InputStream in) throws IOException {
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

    private static void fillBuffer(final InputStream in, final byte[] buffer, final int length) throws IOException {
        int bytesRead;
        int totalBytesRead = 0;
        while ((bytesRead = in.read(buffer, totalBytesRead, length - totalBytesRead)) > 0) {
            totalBytesRead += bytesRead;
        }
        if (totalBytesRead != length) {
            throw new EOFException();
        }
    }
}

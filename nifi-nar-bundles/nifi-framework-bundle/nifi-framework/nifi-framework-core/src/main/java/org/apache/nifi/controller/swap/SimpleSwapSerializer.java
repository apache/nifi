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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated in favor of using {@link SchemaSwapSerializer}.
 */
@Deprecated
public class SimpleSwapSerializer implements SwapSerializer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleSwapSerializer.class);
    public static final int SWAP_ENCODING_VERSION = 10;


    @Override
    public void serializeFlowFiles(final List<FlowFileRecord> toSwap, final FlowFileQueue queue, final String swapLocation, final OutputStream destination) throws IOException {
        if (toSwap == null || toSwap.isEmpty()) {
            return;
        }

        long contentSize = 0L;
        for (final FlowFileRecord record : toSwap) {
            contentSize += record.getSize();
        }

        // persist record to disk via the swap file
        final DataOutputStream out = new DataOutputStream(destination);
        try {
            out.writeInt(SWAP_ENCODING_VERSION);
            out.writeUTF(queue.getIdentifier());
            out.writeInt(toSwap.size());
            out.writeLong(contentSize);

            // get the max record id and write that out so that we know it quickly for restoration
            long maxRecordId = 0L;
            for (final FlowFileRecord flowFile : toSwap) {
                if (flowFile.getId() > maxRecordId) {
                    maxRecordId = flowFile.getId();
                }
            }

            out.writeLong(maxRecordId);

            for (final FlowFileRecord flowFile : toSwap) {
                out.writeLong(flowFile.getId());
                out.writeLong(flowFile.getEntryDate());
                out.writeLong(flowFile.getLineageStartDate());
                out.writeLong(flowFile.getLineageStartIndex());
                out.writeLong(flowFile.getLastQueueDate());
                out.writeLong(flowFile.getQueueDateIndex());
                out.writeLong(flowFile.getSize());

                final ContentClaim claim = flowFile.getContentClaim();
                if (claim == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    final ResourceClaim resourceClaim = claim.getResourceClaim();
                    out.writeUTF(resourceClaim.getId());
                    out.writeUTF(resourceClaim.getContainer());
                    out.writeUTF(resourceClaim.getSection());
                    out.writeLong(claim.getOffset());
                    out.writeLong(claim.getLength());
                    out.writeLong(flowFile.getContentClaimOffset());
                    out.writeBoolean(resourceClaim.isLossTolerant());
                }

                final Map<String, String> attributes = flowFile.getAttributes();
                out.writeInt(attributes.size());
                for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                    writeString(entry.getKey(), out);
                    writeString(entry.getValue(), out);
                }
            }
        } finally {
            out.flush();
        }

        logger.info("Successfully swapped out {} FlowFiles from {} to Swap File {}", toSwap.size(), queue, swapLocation);
    }

    private void writeString(final String toWrite, final OutputStream out) throws IOException {
        final byte[] bytes = toWrite.getBytes(StandardCharsets.UTF_8);
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

    @Override
    public String getSerializationName() {
        return "Simple Swap Serializer";
    }
}

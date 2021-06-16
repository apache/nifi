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

package org.apache.nifi.controller;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

public class MockFlowFileRecord implements FlowFileRecord {
    private static final AtomicLong idGenerator = new AtomicLong(0L);

    private final long id = idGenerator.getAndIncrement();
    private final long entryDate = System.currentTimeMillis();
    private final Map<String, String> attributes;
    private final long size;
    private final ContentClaim contentClaim;

    public MockFlowFileRecord() {
        this(1L);
    }

    public MockFlowFileRecord(final long size) {
        this(new HashMap<>(), size);
    }

    public MockFlowFileRecord(final Map<String, String> attributes, final long size) {
        this(attributes, size, null);
    }

    public MockFlowFileRecord(final Map<String, String> attributes, final long size, final ContentClaim contentClaim) {
        this.attributes = attributes;
        this.size = size;
        this.contentClaim = contentClaim;

        if (!attributes.containsKey(CoreAttributes.UUID.key())) {
            attributes.put(CoreAttributes.UUID.key(), createFakeUUID());
        }
    }

    public static void resetIdGenerator() {
        idGenerator.set(0L);
    }

    private String createFakeUUID() {
        final String s = Long.toHexString(id);
        return new StringBuffer("00000000-0000-0000-0000000000000000".substring(0, (35 - s.length())) + s).insert(23, '-').toString();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public long getEntryDate() {
        return entryDate;
    }

    @Override
    public long getLineageStartDate() {
        return entryDate;
    }

    @Override
    public Long getLastQueueDate() {
        return null;
    }

    @Override
    public boolean isPenalized() {
        return false;
    }

    @Override
    public String getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public int compareTo(final FlowFile o) {
        return Long.compare(id, o.getId());
    }

    @Override
    public long getPenaltyExpirationMillis() {
        return 0;
    }

    @Override
    public ContentClaim getContentClaim() {
        return contentClaim;
    }

    @Override
    public long getContentClaimOffset() {
        return 0;
    }

    @Override
    public long getLineageStartIndex() {
        return 0;
    }

    @Override
    public long getQueueDateIndex() {
        return 0;
    }
}

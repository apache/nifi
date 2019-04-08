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
package org.apache.nifi.stateless.core;

import com.google.gson.JsonObject;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.stateless.bootstrap.InMemoryFlowFile;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class StatelessFlowFile implements InMemoryFlowFile {

    private final Map<String, String> attributes = new HashMap<>();
    private static AtomicLong nextID = new AtomicLong(0);

    public final boolean materializeContent;
    private long id;
    private final long entryDate;
    private final long creationTime;
    private boolean penalized = false;

    private List<InputStream> dataStreams = new ArrayList<>();
    private byte[] data = new byte[0];
    private boolean isFullyMaterialized = true;

    private long lastEnqueuedDate = 0;
    private long enqueuedIndex = 0;

    public StatelessFlowFile(String content, Map<String, String> attributes, boolean materializeContent) {
        this(content.getBytes(StandardCharsets.UTF_8), attributes, materializeContent);
    }

    public StatelessFlowFile(byte[] content, Map<String, String> attributes, boolean materializeContent) {
        this(materializeContent);

        this.attributes.putAll(attributes);
        this.setData(content);
    }

    public StatelessFlowFile(final StatelessFlowFile toCopy, boolean materializeContent) {
        this(materializeContent);
        this.id = toCopy.id;

        attributes.putAll(toCopy.getAttributes());
        this.penalized = toCopy.isPenalized();

        try {
            this.setData(toCopy.getDataArray());
        } catch (IOException e) {
            throw new FlowFileAccessException("Exception creating FlowFile", e);
        }
    }

    public StatelessFlowFile(final StatelessFlowFile toCopy, long offset, long size, boolean materializeContent) {
        this(materializeContent);
        this.id = toCopy.id;

        attributes.putAll(toCopy.getAttributes());
        this.penalized = toCopy.isPenalized();

        try {
            this.setData(Arrays.copyOfRange(toCopy.getDataArray(), (int) offset, (int) (offset + size)));
        } catch (IOException e) {
            throw new FlowFileAccessException("Exception creating FlowFile", e);
        }
    }

    public StatelessFlowFile(boolean materializeContent) {
        this.materializeContent = materializeContent;
        this.creationTime = System.nanoTime();
        this.id = nextID.getAndIncrement();
        this.entryDate = System.currentTimeMillis();
        this.lastEnqueuedDate = entryDate;
        attributes.put(CoreAttributes.FILENAME.key(), String.valueOf(System.nanoTime()) + ".statelessFlowFile");
        attributes.put(CoreAttributes.PATH.key(), "target");
        attributes.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());
    }

    //region SimpleMethods
    void setPenalized(boolean penalized) {
        this.penalized = penalized;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getLineageStartDate() {
        return entryDate;
    }

    @Override
    public int compareTo(final FlowFile o) {
        return getAttribute(CoreAttributes.UUID.key()).compareTo(o.getAttribute(CoreAttributes.UUID.key()));
    }

    @Override
    public String getAttribute(final String attrName) {
        return attributes.get(attrName);
    }

    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public long getEntryDate() {
        return entryDate;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isPenalized() {
        return penalized;
    }

    public void putAttributes(final Map<String, String> attrs) {
        attributes.putAll(attrs);
    }

    public void removeAttributes(final Set<String> attrNames) {
        for (final String attrName : attrNames) {
            attributes.remove(attrName);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof FlowFile) {
            return ((FlowFile) obj).getId() == this.id;
        }
        return false;
    }

    @Override
    public Long getLastQueueDate() {
        return lastEnqueuedDate;
    }

    public void setLastEnqueuedDate(long lastEnqueuedDate) {
        this.lastEnqueuedDate = lastEnqueuedDate;
    }

    @Override
    public long getPenaltyExpirationMillis() {
        return -1;
    }

    @Override
    public ContentClaim getContentClaim() {
        return null;
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
        return enqueuedIndex;
    }

    public void setEnqueuedIndex(long enqueuedIndex) {
        this.enqueuedIndex = enqueuedIndex;
    }
    //endregion Methods

    @Override
    public String toString() {
        JsonObject attributes = new JsonObject();
        this.attributes.forEach(attributes::addProperty);

        JsonObject result = new JsonObject();
        result.add("attributes", attributes);

        return result.toString();
    }

    public String toStringFull() {
        JsonObject attributes = new JsonObject();
        this.attributes.forEach(attributes::addProperty);

        JsonObject result = new JsonObject();
        result.add("attributes", attributes);
        try {
            result.addProperty("content", new String(this.getDataArray(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            result.addProperty("content", "Exception getting content: " + e.getMessage());
        }

        return result.toString();
    }

    @Override
    public long getSize() {
        if (isFullyMaterialized)
            return data.length;
        else
            return 0;
    }

    public void addData(final byte[] data) {
        if (materializeContent) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                outputStream.write(this.data);
                outputStream.write(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.data = outputStream.toByteArray();
            isFullyMaterialized = true;
        } else {
            isFullyMaterialized = false;
            this.dataStreams.add(new ByteArrayInputStream(data));
        }
    }

    public void addData(final InputStream in) {
        if (materializeContent) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                outputStream.write(this.data);
                StreamUtils.copy(in, outputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.data = outputStream.toByteArray();
            isFullyMaterialized = true;
        } else {
            isFullyMaterialized = false;
            this.dataStreams.add(in);
        }
    }

    public void setData(final byte[] data) {
        this.data = data;
        isFullyMaterialized = true;
    }

    public void setData(final InputStream in) {
        if (materializeContent) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                StreamUtils.copy(in, outputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.data = outputStream.toByteArray();
            isFullyMaterialized = true;
        } else {
            isFullyMaterialized = false;
            this.dataStreams = new ArrayList<>();
            this.dataStreams.add(in);
        }
    }

    public InputStream getDataStream() {
        if (isFullyMaterialized) {
            return new ByteArrayInputStream(this.data);
        } else {
            return new SequenceInputStream(
                new ByteArrayInputStream(this.data),
                new SequenceInputStream(Collections.enumeration(this.dataStreams))
            );
        }
    }

    public byte[] getDataArray() throws IOException {
        if (!isFullyMaterialized) {
            materializeData();
        }

        return this.data;
    }

    public void materializeData() throws IOException {
        InputStream in = this.getDataStream();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024 * 1024];
        int read = 0;
        while ((read = in.read(buffer)) != -1) {
            baos.write(buffer, 0, read);
        }
        baos.flush();
        this.data = baos.toByteArray();
        this.dataStreams = new ArrayList<>();
        isFullyMaterialized = true;
    }

}
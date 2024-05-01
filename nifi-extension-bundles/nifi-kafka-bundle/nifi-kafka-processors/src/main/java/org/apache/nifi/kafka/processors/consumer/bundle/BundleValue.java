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
package org.apache.nifi.kafka.processors.consumer.bundle;

import java.io.ByteArrayOutputStream;

public class BundleValue {
    private final ByteArrayOutputStream bos;
    private final long firstOffset;
    private long lastOffset;
    private long count;

    public BundleValue(final long offset) {
        this.bos = new ByteArrayOutputStream();
        firstOffset = offset;
        lastOffset = offset;
        count = 0;
    }

    public void update(final byte[] demarcator, final byte[] data, final long offset) {
        if (bos.size() > 0) {
            bos.writeBytes(demarcator);
        }
        bos.writeBytes(data);
        lastOffset = offset;
        ++count;
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public long getCount() {
        return count;
    }

    public byte[] getData() {
        return bos.toByteArray();
    }
}

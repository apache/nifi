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
package org.apache.nifi.remote.protocol;

import org.apache.nifi.remote.exception.HandshakeException;

public class HandshakeProperties {

    private String commsIdentifier;
    private String transitUriPrefix = null;
    private boolean useGzip;
    private long expirationMillis;
    private int batchCount = 0;
    private long batchBytes = 0L;
    private long batchDurationNanos = 0L;

    public String getCommsIdentifier() {
        return commsIdentifier;
    }

    public void setCommsIdentifier(final String commsIdentifier) {
        this.commsIdentifier = commsIdentifier;
    }

    public String getTransitUriPrefix() {
        return transitUriPrefix;
    }

    public void setTransitUriPrefix(final String transitUriPrefix) {
        this.transitUriPrefix = transitUriPrefix;
    }

    public boolean isUseGzip() {
        return useGzip;
    }

    public void setUseGzip(final Boolean useGzip) {
        this.useGzip = useGzip;
    }

    public long getExpirationMillis() {
        return expirationMillis;
    }

    public void setExpirationMillis(final long expirationMillis) {
        this.expirationMillis = expirationMillis;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(final int batchCount) throws HandshakeException {
        if (batchCount < 0) {
            throw new HandshakeException("Cannot request Batch Count less than 1; requested value: " + batchCount);
        }
        this.batchCount = batchCount;
    }

    public long getBatchBytes() {
        return batchBytes;
    }

    public void setBatchBytes(final long batchBytes) throws HandshakeException {
        if (batchBytes < 0) {
            throw new HandshakeException("Cannot request Batch Size less than 1; requested value: " + batchBytes);
        }
        this.batchBytes = batchBytes;
    }

    public long getBatchDurationNanos() {
        return batchDurationNanos;
    }

    public void setBatchDurationNanos(final long batchDurationNanos) throws HandshakeException {
        if (batchDurationNanos < 0) {
            throw new HandshakeException("Cannot request Batch Duration less than 1; requested value: " + batchDurationNanos);
        }
        this.batchDurationNanos = batchDurationNanos;
    }
}

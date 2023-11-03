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
package org.apache.nifi.provenance.upload;

public class StandardUploadContext implements UploadContext {
    private String transitUri;
    private String details;
    private long transmissionMillis;
    private boolean force;

    private StandardUploadContext() {
        // Private constructor to enforce the use of the builder
    }

    @Override
    public String getTransitUri() {
        return transitUri;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public long getTransmissionMillis() {
        return transmissionMillis;
    }

    @Override
    public boolean isForce() {
        return force;
    }

    public static class Builder {
        private final String transitUri;
        private String details;
        private long transmissionMillis;
        private boolean force;

        public Builder(String transitUri) {
            this.transitUri = transitUri;
        }

        public Builder details(String details) {
            this.details = details;
            return this;
        }

        public Builder transmissionMillis(long transmissionMillis) {
            this.transmissionMillis = transmissionMillis;
            return this;
        }

        public Builder force(boolean force) {
            this.force = force;
            return this;
        }

        public StandardUploadContext build() {
            StandardUploadContext event = new StandardUploadContext();
            event.transitUri = transitUri;
            event.details = details;
            event.transmissionMillis = transmissionMillis;
            event.force = force;
            return event;
        }
    }
}


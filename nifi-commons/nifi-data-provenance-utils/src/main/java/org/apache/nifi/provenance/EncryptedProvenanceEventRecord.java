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
package org.apache.nifi.provenance;

import java.util.Date;

public class EncryptedProvenanceEventRecord extends StandardProvenanceEventRecord {

    private String keyId;
    private String algorithm;
    private String version;

    private EncryptedProvenanceEventRecord(final Builder builder) {
        super(builder);

    }

    @Override
    public String toString() {
        return "EncryptedProvenanceEventRecord ["
                + "eventId=" + getEventId()
                + ", eventType=" + getEventType()
                + ", eventTime=" + new Date(getEventTime())
                + ", uuid=" + getFlowFileUuid()
                + ", fileSize=" + getFileSize()
                + ", componentId=" + getComponentId()
                + ", transitUri=" + getTransitUri()
                + ", sourceSystemFlowFileIdentifier=" + getSourceSystemFlowFileIdentifier()
                + ", parentUuids=" + getParentUuids()
                + ", alternateIdentifierUri=" + getAlternateIdentifierUri()
                + ", keyId=" + getKeyId()
                + ", algorithm=" + getAlgorithm()
                + ", version=" + getVersion()
                + "]";
    }

    public String getKeyId() {
        return keyId;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && encryptionParamsEqual(o);
    }

    private boolean encryptionParamsEqual(Object o) {
        EncryptedProvenanceEventRecord other = (EncryptedProvenanceEventRecord) o;
        return getKeyId().equals(other.getKeyId()) &&
                getAlgorithm().equals(other.getAlgorithm()) &&
                getVersion().equals(other.getVersion());

    }
}

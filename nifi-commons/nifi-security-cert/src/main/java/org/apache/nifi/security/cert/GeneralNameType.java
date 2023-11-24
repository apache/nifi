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
package org.apache.nifi.security.cert;

/**
 * Subject Alternative Name General Name Types defined according to RFC 3280 Section 4.2.1.7
 */
public enum GeneralNameType {
    OTHER_NAME(0, "otherName", byte[].class),

    RFC_822_NAME(1, "rfc822Name", String.class),

    DNS_NAME(2, "dNSName", String.class),

    X400_ADDRESS(3, "x400Address", byte[].class),

    DIRECTORY_NAME(4, "directoryName", String.class),

    EDI_PARTY_NAME(5, "ediPartyName", String.class),

    UNIFORM_RESOURCE_IDENTIFIER(6, "uniformResourceIdentifier", String.class),

    IP_ADDRESS(7, "iPAddress", String.class),

    REGISTERED_ID(8, "registeredID", byte[].class);

    private final int nameType;

    private final String generalName;

    private final Class<?> valueClass;

    GeneralNameType(final int nameType, final String generalName, final Class<?> valueClass) {
        this.nameType = nameType;
        this.generalName = generalName;
        this.valueClass = valueClass;
    }

    /**
     * Get General Name Type identifier
     *
     * @return Subject Alternative Name Type identifier
     */
    public int getNameType() {
        return nameType;
    }

    /**
     * Get General Name label
     *
     * @return General Name label from RFC 3280 Section 4.2.1.7
     */
    public String getGeneralName() {
        return generalName;
    }

    /**
     * Get Value Class associated with General Name elements returned from Certificates
     *
     * @return Value Class
     */
    public Class<?> getValueClass() {
        return valueClass;
    }
}

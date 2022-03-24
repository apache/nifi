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
package org.apache.nifi.web.security.jwt.key.service;

import java.time.Instant;

/**
 * Verification Key used for storing serialized instances
 */
class VerificationKey {
    private String id;

    private String algorithm;

    private byte[] encoded;

    private Instant expiration;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(final String algorithm) {
        this.algorithm = algorithm;
    }

    public byte[] getEncoded() {
        return encoded;
    }

    public void setEncoded(final byte[] encoded) {
        this.encoded = encoded;
    }

    public Instant getExpiration() {
        return expiration;
    }

    public void setExpiration(final Instant expiration) {
        this.expiration = expiration;
    }
}

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
package org.apache.nifi.web.security.jwt.jws;

import java.time.Instant;

/**
 * JSON Web Signature Signer Provider supports accessing signer and identifier properties
 */
public interface JwsSignerProvider {
    /**
     * Get JSON Web Signature Signer Container
     *
     * @param expiration New JSON Web Token Expiration to be set for the returned Signer
     * @return JSON Web Signature Signer Container
     */
    JwsSignerContainer getJwsSignerContainer(Instant expiration);
}

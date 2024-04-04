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

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import org.apache.nifi.web.security.jwt.key.VerificationKeySelector;

import java.security.Key;
import java.util.List;

/**
 * Standard JSON Web Signature Key Selector for selecting keys using Key Identifier
 * @param <C> Security Context
 */
public class StandardJWSKeySelector<C extends SecurityContext> implements JWSKeySelector<C> {
    private final VerificationKeySelector verificationKeySelector;

    /**
     * Standard JSON Web Signature Key Selector constructor requires a Verification Key Selector
     * @param verificationKeySelector Verification Key Selector
     */
    public StandardJWSKeySelector(final VerificationKeySelector verificationKeySelector) {
        this.verificationKeySelector = verificationKeySelector;
    }

    /**
     * Select JSON Web Signature Key using Key Identifier from JWS Header
     *
     * @param jwsHeader JSON Web Signature Header
     * @param context Context not used
     * @return List of found java.security.Key objects
     */
    @Override
    public List<? extends Key> selectJWSKeys(final JWSHeader jwsHeader, final C context) {
        final String keyId = jwsHeader.getKeyID();
        return verificationKeySelector.getVerificationKeys(keyId);
    }
}

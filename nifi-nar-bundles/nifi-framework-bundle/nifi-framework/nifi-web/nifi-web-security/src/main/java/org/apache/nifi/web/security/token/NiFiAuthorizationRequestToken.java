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
package org.apache.nifi.web.security.token;

import java.util.Collections;
import java.util.List;
import org.springframework.security.authentication.AbstractAuthenticationToken;

/**
 * An authentication token that is used as an authorization request. The request has already been authenticated and is now going to be authorized.
 * The request chain is specified during creation and is used authorize the user(s).
 */
public class NiFiAuthorizationRequestToken extends AbstractAuthenticationToken {

    private final List<String> chain;

    public NiFiAuthorizationRequestToken(final List<String> chain) {
        super(null);
        this.chain = chain;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return chain;
    }

    public List<String> getChain() {
        return Collections.unmodifiableList(chain);
    }

    @Override
    public final void setAuthenticated(boolean authenticated) {
        throw new IllegalArgumentException("Cannot change the authenticated state.");
    }
}

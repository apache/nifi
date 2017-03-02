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

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * An authentication token that represents an Authenticated and Authorized user of the NiFi Apis. The authorities are based off the specified UserDetails.
 */
public class NiFiAuthenticationToken extends AbstractAuthenticationToken {

    final UserDetails nifiUserDetails;

    public NiFiAuthenticationToken(final UserDetails nifiUserDetails) {
        super(nifiUserDetails.getAuthorities());
        super.setAuthenticated(true);
        setDetails(nifiUserDetails);
        this.nifiUserDetails = nifiUserDetails;
    }

    @Override
    public Object getCredentials() {
        return nifiUserDetails.getPassword();
    }

    @Override
    public Object getPrincipal() {
        return nifiUserDetails;
    }

    @Override
    public final void setAuthenticated(boolean authenticated) {
        throw new IllegalArgumentException("Cannot change the authenticated state.");
    }

    @Override
    public String toString() {
        return nifiUserDetails.getUsername();
    }
}

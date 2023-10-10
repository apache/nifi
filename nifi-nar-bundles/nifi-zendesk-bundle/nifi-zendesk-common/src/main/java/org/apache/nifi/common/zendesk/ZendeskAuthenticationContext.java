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
package org.apache.nifi.common.zendesk;

public class ZendeskAuthenticationContext {

    private final String subdomain;
    private final String user;
    private final ZendeskAuthenticationType authenticationType;
    private final String authenticationCredentials;

    public ZendeskAuthenticationContext(String subdomain, String user, ZendeskAuthenticationType authenticationType, String authenticationCredentials) {
        this.subdomain = subdomain;
        this.user = user;
        this.authenticationType = authenticationType;
        this.authenticationCredentials =authenticationCredentials;
    }

    public String getSubdomain() {
        return subdomain;
    }

    public String getUser() {
        return user;
    }

    public ZendeskAuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    public String getAuthenticationCredentials() {
        return authenticationCredentials;
    }
}

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
package org.apache.nifi.registry.security.authentication;

import java.io.Serializable;

public class AuthenticationRequest implements Serializable {

    private String username;
    private Object credentials;
    private Object details;

    public AuthenticationRequest(final String username, final Object credentials, final Object details) {
        this.username = username;
        this.credentials = credentials;
        this.details = details;
    }

    public AuthenticationRequest() { }

    public String getUsername() {
        return username;
    }

    public void setUsername(final String username) {
        this.username = username;
    }

    public Object getCredentials() {
        return credentials;
    }

    public void setCredentials(final Object credentials) {
        this.credentials = credentials;
    }

    public Object getDetails() {
        return details;
    }

    public void setDetails(final Object details) {
        this.details = details;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AuthenticationRequest that = (AuthenticationRequest) o;

        return username != null ? username.equals(that.username) : that.username == null;
    }

    @Override
    public int hashCode() {
        return username != null ? username.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "AuthenticationRequest{" +
                "username='" + username + '\'' +
                ", credentials=[PROTECTED]" +
                ", details=" + details +
                '}';
    }
}

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
package org.apache.nifi.web.security.logout;

import org.apache.commons.lang3.Validate;

import java.util.Objects;

public class LogoutRequest {

    private final String requestIdentifier;

    private final String mappedUserIdentity;

    public LogoutRequest(final String requestIdentifier, final String mappedUserIdentity) {
        this.requestIdentifier = Validate.notBlank(requestIdentifier, "Request identifier is required");
        this.mappedUserIdentity = Validate.notBlank(mappedUserIdentity, "User identity is required");
    }

    public String getRequestIdentifier() {
        return requestIdentifier;
    }

    public String getMappedUserIdentity() {
        return mappedUserIdentity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.requestIdentifier, this.mappedUserIdentity);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof LogoutRequest)) {
            return false;
        }

        final LogoutRequest other = (LogoutRequest) obj;
        return Objects.equals(this.requestIdentifier, other.requestIdentifier)
                && Objects.equals(this.mappedUserIdentity, other.mappedUserIdentity);
    }
}

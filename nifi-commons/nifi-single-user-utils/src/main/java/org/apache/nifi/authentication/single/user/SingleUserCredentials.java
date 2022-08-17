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
package org.apache.nifi.authentication.single.user;

import java.util.Objects;

/**
 * Single User Credentials used for writing updated configuration
 */
public class SingleUserCredentials {
    private final String username;

    private final String password;

    private final String providerClass;

    public SingleUserCredentials(final String username, final String password, final String providerClass) {
        this.username = Objects.requireNonNull(username, "Username required");
        this.password = Objects.requireNonNull(password, "Password required");
        this.providerClass = Objects.requireNonNull(providerClass, "Provider Class required");
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getProviderClass() {
        return providerClass;
    }
}

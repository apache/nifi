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
package org.apache.nifi.processors.snowflake;

import java.security.PrivateKey;

public class SnowflakeConfiguration {

    private final String accountUrl;
    private final String account;
    private final String username;
    private final PrivateKey privateKey;
    private final String role;

    public SnowflakeConfiguration(String accountUrl, String account, String username, PrivateKey privateKey, String role) {
        this.accountUrl = accountUrl;
        this.account = account;
        this.username = username;
        this.privateKey = privateKey;
        this.role = role;
    }

    public String getAccountUrl() {
        return accountUrl;
    }

    public String getAccount() {
        return account;
    }

    public String getUsername() {
        return username;
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    public String getRole() {
        return role;
    }
}

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
package org.apache.nifi.processors.salesforce.rest;

import java.util.function.Supplier;

public final class SalesforceConfiguration {

    private final String instanceUrl;
    private final String version;
    private final Supplier<String> accessTokenProvider;
    private final int responseTimeoutMillis;

    private SalesforceConfiguration(String instanceUrl, String version, Supplier<String> accessTokenProvider, int responseTimeoutMillis) {
        this.instanceUrl = instanceUrl;
        this.version = version;
        this.accessTokenProvider = accessTokenProvider;
        this.responseTimeoutMillis = responseTimeoutMillis;
    }

    public static SalesforceConfiguration create(String instanceUrl, String version, Supplier<String> accessTokenProvider, int responseTimeoutMillis) {
        return new SalesforceConfiguration(instanceUrl, version, accessTokenProvider, responseTimeoutMillis);
    }

    public String getInstanceUrl() {
        return instanceUrl;
    }

    public String getVersion() {
        return version;
    }

    public Supplier<String> getAccessTokenProvider() {
        return accessTokenProvider;
    }

    public int getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }
}

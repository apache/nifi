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
package org.apache.nifi.processors.twitter;

public enum StreamEndpoint {
    SAMPLE_ENDPOINT("Sample Endpoint", "/2/tweets/sample/stream"),
    SEARCH_ENDPOINT("Search Endpoint", "/2/tweets/search/stream");

    private String endpointName;
    private String path;

    StreamEndpoint(final String endpointName, final String path) {
        this.endpointName = endpointName;
        this.path = path;
    }

    public String getEndpointName() {
        return this.endpointName;
    }

    public String getPath() {
        return this.path;
    }
}
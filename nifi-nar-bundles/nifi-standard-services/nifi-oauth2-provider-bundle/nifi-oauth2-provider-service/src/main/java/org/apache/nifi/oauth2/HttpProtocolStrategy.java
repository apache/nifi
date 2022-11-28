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
package org.apache.nifi.oauth2;

import org.apache.nifi.components.DescribedValue;

import java.util.List;

import okhttp3.Protocol;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * HTTP protocol configuration strategy
 */
public enum HttpProtocolStrategy implements DescribedValue {
    HTTP_1_1("http/1.1", "HTTP/1.1", singletonList(Protocol.HTTP_1_1)),

    H2_HTTP_1_1("h2 http/1.1", "HTTP/2 and HTTP/1.1 negotiated based on requested protocols", asList(Protocol.HTTP_1_1, Protocol.HTTP_2)),

    H2("h2", "HTTP/2", singletonList(Protocol.HTTP_2));

    private final String displayName;

    private final String description;

    private final List<Protocol> protocols;

    HttpProtocolStrategy(final String displayName, final String description, final List<Protocol> protocols) {
        this.displayName = displayName;
        this.description = description;
        this.protocols = protocols;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public List<Protocol> getProtocols() {
        return protocols;
    }
}

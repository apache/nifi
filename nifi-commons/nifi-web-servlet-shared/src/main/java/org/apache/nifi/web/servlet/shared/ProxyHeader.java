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
package org.apache.nifi.web.servlet.shared;

/**
 * Enumeration of supported Proxy Headers that provide information about the original request properties
 */
public enum ProxyHeader {
    HOST("Host"),

    PROXY_CONTEXT_PATH("X-ProxyContextPath"),

    PROXY_SCHEME("X-ProxyScheme"),

    PROXY_HOST("X-ProxyHost"),

    PROXY_PORT("X-ProxyPort"),

    FORWARDED_CONTEXT("X-Forwarded-Context"),

    FORWARDED_PREFIX("X-Forwarded-Prefix"),

    FORWARDED_PROTO("X-Forwarded-Proto"),

    FORWARDED_HOST("X-Forwarded-Host"),

    FORWARDED_PORT("X-Forwarded-Port");

    private final String header;

    ProxyHeader(final String header) {
        this.header = header;
    }

    public String getHeader() {
        return header;
    }
}

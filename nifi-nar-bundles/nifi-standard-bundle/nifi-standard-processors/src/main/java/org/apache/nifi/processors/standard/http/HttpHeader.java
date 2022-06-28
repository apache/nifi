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
package org.apache.nifi.processors.standard.http;

/**
 * HTTP Header reusable enumerated values
 */
public enum HttpHeader {
    /** Authorization defined in RFC 7235 Section 4.2 */
    AUTHORIZATION("Authorization"),

    /** Content-Encoding defined in RFC 7231 Section 3.1.2.2 */
    CONTENT_ENCODING("Content-Encoding"),

    /** Date defined in RFC 7231 Section 7.1.1.2 */
    DATE("Date"),

    /** User-Agent defined in RFC 7231 Section 5.5.3 */
    USER_AGENT("User-Agent");

    private final String header;

    HttpHeader(final String header) {
        this.header = header;
    }

    public String getHeader() {
        return header;
    }
}

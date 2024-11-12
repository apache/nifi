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
package org.apache.nifi.web.client.api;

import java.io.InputStream;
import java.util.OptionalLong;

/**
 * HTTP Request Body Specification builder
 */
public interface HttpRequestBodySpec extends HttpRequestHeadersSpec {
    /**
     * Set Request Body as stream
     *
     * @param inputStream Request Body stream is required
     * @param contentLength Content Length or empty when not known
     * @return HTTP Request Headers Specification builder
     */
    HttpRequestHeadersSpec body(InputStream inputStream, OptionalLong contentLength);

    /**
     * Set Request Body as provided string encoded as UTF-8.
     * This should be used only when the payload is small. For large amount of data,
     * @see HttpRequestBodySpec#body(InputStream, OptionalLong)
     *
     * @param body String representation of the payload encoded as UTF-8
     * @return HTTP Request Headers Specification builder
     */
    HttpRequestHeadersSpec body(String body);
}

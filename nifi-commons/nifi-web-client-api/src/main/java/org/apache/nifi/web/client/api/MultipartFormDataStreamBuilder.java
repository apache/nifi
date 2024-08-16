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

/**
 * Multipart Form Data Stream Builder supports construction of an Input Stream with form-data sections according to RFC 7578
 */
public interface MultipartFormDataStreamBuilder {
    /**
     * Build Input Stream based on current component elements
     *
     * @return Input Stream
     */
    InputStream build();

    /**
     * Get Content-Type Header value containing multipart/form-data with boundary
     *
     * @return Multipart HTTP Content-Type
     */
    HttpContentType getHttpContentType();

    /**
     * Add Part using specified Name with Content-Type and Stream
     *
     * @param name Name field of part to be added
     * @param httpContentType Content-Type of part to be added
     * @param inputStream Stream content of part to be added
     * @return Builder
     */
    MultipartFormDataStreamBuilder addPart(String name, HttpContentType httpContentType, InputStream inputStream);

    /**
     * Add Part using specified Name with Content-Type and byte array
     *
     * @param name Name field of part to be added
     * @param httpContentType Content-Type of part to be added
     * @param bytes Byte array content of part to be added
     * @return Builder
     */
    MultipartFormDataStreamBuilder addPart(String name, HttpContentType httpContentType, byte[] bytes);
}

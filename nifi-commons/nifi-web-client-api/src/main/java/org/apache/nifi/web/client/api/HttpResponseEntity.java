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

import java.io.Closeable;
import java.io.InputStream;
import java.net.URI;

/**
 * HTTP Response Entity extends Closeable to handle closing Response Body
 */
public interface HttpResponseEntity extends Closeable {
    /**
     * Get HTTP Response Status Code
     *
     * @return HTTP Response Status Code
     */
    int statusCode();

    /**
     * Get HTTP Response Headers
     *
     * @return HTTP Response Headers
     */
    HttpEntityHeaders headers();

    /**
     * Get HTTP Response Body stream
     *
     * @return HTTP Response Body stream can be empty
     */
    InputStream body();

    /**
     * Get the endpoint URI that was accessed to generate this HTTP response
     *
     * @return HTTP URI from which the response was retrieved
     */
    URI uri();
}

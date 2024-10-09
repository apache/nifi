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

package org.apache.nifi.runtime.util;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class HttpExchangeUtils {
    private static final Logger logger = LoggerFactory.getLogger(HttpExchangeUtils.class);

    public static void drainRequestBody(final HttpExchange exchange) {
        final byte[] buffer = new byte[4096];
        try (final InputStream in = exchange.getRequestBody()) {
            while ((in.read(buffer)) != -1) {
                // Ignore the data read, just drain the input stream
            }
        } catch (final IOException ioe) {
            // Since we don't actually care about the contents of the input, we will ignore any Exceptions when reading from it.
            logger.debug("Failed to fully drain HttpExchange InputStream from {}", exchange.getRequestURI(), ioe);
        }
    }

}

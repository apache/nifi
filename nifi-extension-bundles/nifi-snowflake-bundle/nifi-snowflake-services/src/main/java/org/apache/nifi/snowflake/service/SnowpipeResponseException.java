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

package org.apache.nifi.snowflake.service;

/**
 * Exception for Snowpipe REST API failures including HTTP error responses and communication failures
 */
class SnowpipeResponseException extends RuntimeException {

    /**
     * Snowpipe Response Exception with message describing the failure
     *
     * @param message Failure description including HTTP status code when applicable
     */
    public SnowpipeResponseException(final String message) {
        super(message);
    }

    /**
     * Snowpipe Response Exception with message and underlying cause
     *
     * @param message Failure description
     * @param cause Underlying exception such as IOException for transport failures
     */
    public SnowpipeResponseException(final String message, final Throwable cause) {
        super(message, cause);
    }
}

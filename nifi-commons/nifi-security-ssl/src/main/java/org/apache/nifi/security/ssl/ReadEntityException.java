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
package org.apache.nifi.security.ssl;

/**
 * Exception indicating runtime failure to read specified entity
 */
public class ReadEntityException extends RuntimeException {
    /**
     * Read Entity Exception Constructor with standard properties
     *
     * @param message Exception Message
     * @param cause Exception Cause
     */
    public ReadEntityException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Read Entity Exception Constructor without Throwable cause
     *
     * @param message Exception Message
     */
    public ReadEntityException(final String message) {
        super(message);
    }
}

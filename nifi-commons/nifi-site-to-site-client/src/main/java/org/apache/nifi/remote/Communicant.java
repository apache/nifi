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
package org.apache.nifi.remote;

/**
 * Represents the remote entity that the client is communicating with
 */
public interface Communicant {

    /**
     * @return the NiFi site-to-site URL for the remote NiFi instance
     */
    String getUrl();

    /**
     * @return The Host of the remote NiFi instance
     */
    String getHost();

    /**
     * @return The Port that the remote NiFi instance is listening on for
     * site-to-site communications
     */
    int getPort();

    /**
     * @return The distinguished name that the remote NiFi instance has provided
     * in its certificate if using secure communications, or <code>null</code>
     * if the Distinguished Name is unknown
     */
    String getDistinguishedName();
}

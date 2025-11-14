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

import java.util.concurrent.TimeUnit;

/**
 * Abstraction for Site-To-Site Destination similar to framework Remote Destination
 */
public interface SiteToSiteDestination {
    /**
     * Get Identifier
     *
     * @return Destination Identifier
     */
    String getIdentifier();

    /**
     * Get Name
     *
     * @return human-readable name of the destination
     */
    String getName();

    /**
     * Get Yield Period
     *
     * @param timeUnit Unit of time for requested Yield Period
     * @return the amount of time that system should pause sending to a
     * particular node if unable to send data to or receive data from this
     * endpoint
     */
    long getYieldPeriod(TimeUnit timeUnit);

    /**
     * Use Compression Status
     *
     * @return whether compression should be used when transferring data
     * to or receiving data from the remote endpoint
     */
    boolean isUseCompression();
}

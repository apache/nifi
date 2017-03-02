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

import java.util.List;

public interface VersionNegotiator {

    /**
     * @return the currently configured Version of this resource
     */
    int getVersion();

    /**
     * Sets the version of this resource to the specified version. Only the lower byte of the version is relevant.
     *
     * @param version the version to set
     * @throws IllegalArgumentException if the given Version is not supported by this resource, as is indicated by the {@link #isVersionSupported(int)} method
     */
    void setVersion(int version) throws IllegalArgumentException;

    /**
     *
     * @return the Version of this resource that is preferred
     */
    int getPreferredVersion();

    /**
     * Gets the preferred version of this resource that is no greater than the given maxVersion. If no acceptable version exists that is less than <code>maxVersion</code>, then <code>null</code> is
     * returned
     *
     * @param maxVersion the maximum version desired
     * @return the preferred version if found; null otherwise
     */
    Integer getPreferredVersion(int maxVersion);

    /**
     * Indicates whether or not the specified version is supported by this resource
     *
     * @param version the version to test
     * @return true if supported; false otherwise
     */
    boolean isVersionSupported(int version);

    List<Integer> getSupportedVersions();
}

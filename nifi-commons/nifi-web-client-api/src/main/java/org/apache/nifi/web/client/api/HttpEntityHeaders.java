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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * HTTP Entity Headers supporting retrieval of single or multiple header values
 */
public interface HttpEntityHeaders {
    /**
     * Get First Header using specified Header Name
     *
     * @param headerName Header Name to be retrieved
     * @return First Header Value or empty when not found
     */
    Optional<String> getFirstHeader(String headerName);

    /**
     * Get Header Values using specified Header Name
     *
     * @param headerName Header Name to be retrieved
     * @return List of Header Values or empty when not found
     */
    List<String> getHeader(String headerName);

    /**
     * Get Header Names
     *
     * @return Collection of Header Names or empty when not found
     */
    Collection<String> getHeaderNames();
}

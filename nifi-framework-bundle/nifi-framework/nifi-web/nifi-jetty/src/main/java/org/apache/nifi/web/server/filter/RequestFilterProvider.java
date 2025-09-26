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
package org.apache.nifi.web.server.filter;

import org.apache.nifi.util.NiFiProperties;
import org.eclipse.jetty.ee11.servlet.FilterHolder;

import java.util.List;

/**
 * Request Filter Provider for abstracting configuration of HTTP Request Filters
 */
public interface RequestFilterProvider {
    /**
     * Get Filters using provided NiFi Properties
     *
     * @param properties NiFi Properties required
     * @return List of Filter Holder
     */
    List<FilterHolder> getFilters(NiFiProperties properties);
}

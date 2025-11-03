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
package org.apache.nifi.services.iceberg.catalog;

import org.apache.iceberg.rest.RESTClient;

import java.util.Map;

/**
 * Abstraction for constructing an Apache Iceberg REST Client from provided properties
 */
@FunctionalInterface
interface RESTClientProvider {
    /**
     * Build Iceberg REST Client using properties
     *
     * @param properties Configuration properties
     * @return REST Client
     */
    RESTClient build(Map<String, String> properties);
}

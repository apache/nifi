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
package org.apache.nifi.service.cql.api.service;

import java.time.Duration;

/**
 * Per-call overrides for one {@link CQLExecutionService#insert} or {@link CQLExecutionService#update}; a
 * {@code null} field defers to the service's configuration.
 *
 * @param ttl the Time To Live to apply, or {@code null} for the service's default
 * @param timestampField a record field supplying that record's CQL write timestamp, or {@code null} for
 * the current time; ignored for Increment/Decrement
 */
public record WriteOverrides(Duration ttl, String timestampField) {
    public static final WriteOverrides NONE = new WriteOverrides(null, null);
}

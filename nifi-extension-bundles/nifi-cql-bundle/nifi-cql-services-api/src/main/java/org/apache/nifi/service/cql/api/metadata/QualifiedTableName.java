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

package org.apache.nifi.service.cql.api.metadata;

import org.apache.nifi.util.StringUtils;

/**
 * A table name, optionally keyspace-qualified, for every table-taking {@code CQLExecutionService} method. An
 * unqualified instance ({@code null}/empty {@code keyspace}) resolves against the connection's default
 * keyspace - the driver session's for statement building, or the service's configured one for schema
 * metadata lookups.
 *
 * @param keyspace the table's keyspace, or {@code null}/empty if unqualified
 * @param table the table name, without keyspace
 */
public record QualifiedTableName(String keyspace, String table) {
    /**
     * @return whether {@code keyspace} is set, so no default needs resolving
     */
    public boolean isQualified() {
        return StringUtils.isNotEmpty(keyspace);
    }
}

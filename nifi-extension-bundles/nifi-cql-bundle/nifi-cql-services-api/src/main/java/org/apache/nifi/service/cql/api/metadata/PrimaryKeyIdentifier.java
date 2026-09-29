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

/**
 * Names one column of one keyspace-qualified table: the key type of the {@code primaryKeyOverrides} map on
 * {@code CQLExecutionService#insert}/{@code update}/{@code delete}, each entry mapping a column to a
 * {@code RecordPath} that resolves its value from a record instead of a same-named field. Caller-supplied
 * (e.g. a {@code PutCQLRecord} dynamic property), so unlike {@link PrimaryKeyMetadata#name()} the
 * {@code fieldName} may not be driver-normalized.
 *
 * @param keyspace the target table's keyspace
 * @param tableName the target table, without keyspace
 * @param fieldName the target column, as the caller supplied it
 */
public record PrimaryKeyIdentifier(String keyspace, String tableName, String fieldName) {
}

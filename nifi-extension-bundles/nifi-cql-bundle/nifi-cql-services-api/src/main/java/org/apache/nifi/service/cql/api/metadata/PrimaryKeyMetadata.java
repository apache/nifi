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

import org.apache.nifi.service.cql.api.constants.PrimaryKeyFieldType;

/**
 * One column of a table's primary key, within a {@link PrimaryKey} from
 * {@code CQLExecutionService#getMetadata}.
 *
 * @param name the column's real, driver-normalized (case-correct, unquoted) name
 * @param position the column's 0-based order within its own group (partition columns, or clustering
 * columns), not a single ordinal across both
 * @param fieldType whether this is a partition key column or a clustering column
 */
public record PrimaryKeyMetadata(String name, int position, PrimaryKeyFieldType fieldType) {
}

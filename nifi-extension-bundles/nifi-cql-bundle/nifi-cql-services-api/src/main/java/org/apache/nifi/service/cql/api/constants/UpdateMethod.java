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

package org.apache.nifi.service.cql.api.constants;

import org.apache.nifi.service.cql.api.service.CQLExecutionService;

/**
 * How {@link CQLExecutionService#update} applies a record's non-key fields. {@code INCREMENT}/{@code DECREMENT}
 * apply only to counter columns and require a {@code COUNTER} or {@code UNLOGGED} batch.
 */
public enum UpdateMethod {
    /** Subtracts a field's value from a counter column. */
    DECREMENT,
    /** Adds a field's value to a counter column. */
    INCREMENT,
    /** Overwrites a column's value. */
    SET
}

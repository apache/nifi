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

package org.apache.nifi.serialization.record.validation;

public enum ValidationErrorType {
    /**
     * A required field (i.e., a field that is not 'nullable') exists in the schema, but the record had no value for this field
     * or the value for this field was <code>null</code>.
     */
    MISSING_FIELD,

    /**
     * The record had a field that was not valid according to the schema.
     */
    EXTRA_FIELD,

    /**
     * The record had a value for a field, but the value was not valid according to the schema.
     */
    INVALID_FIELD,

    /**
     * Some other sort of validation error occurred.
     */
    OTHER;
}

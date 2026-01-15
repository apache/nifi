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

package org.apache.nifi.controller.repository.schema;

import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.repository.schema.NamedValue;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;

public class RepositoryRecordUpdate implements Record {
    private final RecordSchema schema;
    private final RepositoryRecordFieldMap fieldMap;

    public RepositoryRecordUpdate(final RepositoryRecordFieldMap fieldMap, final RecordSchema schema) {
        this.schema = schema;
        this.fieldMap = fieldMap;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        if (RepositoryRecordSchema.REPOSITORY_RECORD_UPDATE_V2.equals(fieldName)) {
            final String actionType = (String) fieldMap.getFieldValue(RepositoryRecordSchema.ACTION_TYPE);
            final RepositoryRecordType recordType = RepositoryRecordType.valueOf(actionType);

            final String actionName = switch (recordType) {
                case CREATE, UPDATE -> RepositoryRecordSchema.CREATE_OR_UPDATE_ACTION;
                case DELETE, CONTENTMISSING -> RepositoryRecordSchema.DELETE_ACTION;
                case SWAP_IN -> RepositoryRecordSchema.SWAP_IN_ACTION;
                case SWAP_OUT -> RepositoryRecordSchema.SWAP_OUT_ACTION;
                case SWAP_FILE_DELETED -> RepositoryRecordSchema.SWAP_FILE_DELETED_ACTION;
                case SWAP_FILE_RENAMED -> RepositoryRecordSchema.SWAP_FILE_RENAMED_ACTION;
                default -> throw new IllegalArgumentException("Unknown record type: " + recordType);
            };

            return new NamedValue(actionName, fieldMap);
        }
        return null;
    }

}

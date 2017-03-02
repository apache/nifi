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
import org.wali.UpdateType;

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
            String actionType = (String) fieldMap.getFieldValue(RepositoryRecordSchema.ACTION_TYPE);
            if (RepositoryRecordType.CONTENTMISSING.name().equals(actionType)) {
                actionType = RepositoryRecordType.DELETE.name();
            }
            final UpdateType updateType = UpdateType.valueOf(actionType);

            final String actionName;
            switch (updateType) {
                case CREATE:
                case UPDATE:
                    actionName = RepositoryRecordSchema.CREATE_OR_UPDATE_ACTION;
                    break;
                case DELETE:
                    actionName = RepositoryRecordSchema.DELETE_ACTION;
                    break;
                case SWAP_IN:
                    actionName = RepositoryRecordSchema.SWAP_IN_ACTION;
                    break;
                case SWAP_OUT:
                    actionName = RepositoryRecordSchema.SWAP_OUT_ACTION;
                    break;
                default:
                    return null;
            }

            return new NamedValue(actionName, fieldMap);
        }
        return null;
    }

}

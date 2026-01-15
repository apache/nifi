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

package org.apache.nifi.controller.repository;

import org.wali.SerDe;
import org.wali.UpdateType;

public abstract class RepositoryRecordSerde implements SerDe<SerializedRepositoryRecord> {

    @Override
    public Long getRecordIdentifier(final SerializedRepositoryRecord record) {
        final FlowFileRecord flowFileRecord = record.getFlowFileRecord();
        if (flowFileRecord == null) {
            return null;
        }
        return flowFileRecord.getId();
    }

    @Override
    public UpdateType getUpdateType(final SerializedRepositoryRecord record) {
        return switch (record.getType()) {
            case CONTENTMISSING, DELETE -> UpdateType.DELETE;
            case CREATE -> UpdateType.CREATE;
            case UPDATE -> UpdateType.UPDATE;
            case SWAP_OUT -> UpdateType.SWAP_OUT;
            case SWAP_IN -> UpdateType.SWAP_IN;
            case SWAP_FILE_DELETED -> UpdateType.SWAP_FILE_DELETED;
            case SWAP_FILE_RENAMED -> UpdateType.SWAP_FILE_RENAMED;
            default -> null;
        };
    }

    @Override
    public String getLocation(final SerializedRepositoryRecord record) {
        return record.getSwapLocation();
    }

    @Override
    public String getOriginalLocation(final SerializedRepositoryRecord record) {
        return record.getOriginalSwapLocation();
    }
}

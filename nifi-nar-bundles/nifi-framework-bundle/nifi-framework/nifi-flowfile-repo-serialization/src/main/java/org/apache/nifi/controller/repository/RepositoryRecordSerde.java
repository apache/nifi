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
        return record.getFlowFileRecord().getId();
    }

    @Override
    public UpdateType getUpdateType(final SerializedRepositoryRecord record) {
        switch (record.getType()) {
            case CONTENTMISSING:
            case DELETE:
                return UpdateType.DELETE;
            case CREATE:
                return UpdateType.CREATE;
            case UPDATE:
                return UpdateType.UPDATE;
            case SWAP_OUT:
                return UpdateType.SWAP_OUT;
            case SWAP_IN:
                return UpdateType.SWAP_IN;
        }
        return null;
    }

    @Override
    public String getLocation(final SerializedRepositoryRecord record) {
        return record.getSwapLocation();
    }
}

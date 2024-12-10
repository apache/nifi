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
package org.apache.nifi.controller.status.history.questdb;

import java.util.Arrays;
import java.util.Optional;

enum StorageStatusType {
    CONTENT((short) 0, "contentStorage", "Content Repository"),
    PROVENANCE((short) 1, "provenanceStorage", "Provenance Repository");

    private final short id;
    private final String field;
    private final String label;

    StorageStatusType(final short id, final String field, final String label) {
        this.id = id;
        this.field = field;
        this.label = label;
    }

    static StorageStatusType getById(final int id) {
        final Optional<StorageStatusType> result = Arrays.stream(values()).filter(storageStatusType -> storageStatusType.getId() == id).findFirst();

        if (result.isEmpty()) {
            throw new IllegalArgumentException("Unknown storage type id " + id);
        }

        return result.get();
    }

    short getId() {
        return id;
    }

    String getField() {
        return field;
    }

    String getLabel() {
        return label;
    }
}
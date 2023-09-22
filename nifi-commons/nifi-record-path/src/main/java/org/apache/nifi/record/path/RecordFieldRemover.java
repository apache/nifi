/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.record.path;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldRemovalPath;
import org.apache.nifi.serialization.record.RecordSchema;

public class RecordFieldRemover {
    private final RecordPathCache recordPathCache;
    private final Record record;

    private boolean fieldsChanged;

    public RecordFieldRemover(final Record record, final RecordPathCache recordPathCache) {
        this.record = record;
        this.recordPathCache = recordPathCache;
    }

    public Record getRecord() {
        if (fieldsChanged) {
            record.regenerateSchema();
        }
        return record;
    }

    public void remove(final RecordPathRemovalProperties recordPathRemovalProperties) {
        final RecordPath recordPath = recordPathCache.getCompiled(recordPathRemovalProperties.getRecordPath());
        final RecordPathResult recordPathResult = recordPath.evaluate(record);
        final List<FieldValue> selectedFields = recordPathResult.getSelectedFields().collect(Collectors.toList());

        if (selectedFields.isEmpty()) {
            return;
        }

        if (recordPathRemovalProperties.isAppliedToAllElementsInCollection()) {
            // all elements have the same parent, so navigate up from the first element in the collection
            selectedFields.get(0).getParent().ifPresent(FieldValue::removeContent);
        } else {
            selectedFields.forEach(FieldValue::remove);
        }

        if (recordPathRemovalProperties.isRemovingFieldsNotJustElementsFromWithinCollection()) {
            removeFieldsFromSchema(selectedFields);
        }

        fieldsChanged = true;
    }

    private void removeFieldsFromSchema(final List<FieldValue> selectedFields) {
        final List<RecordFieldRemovalPath> paths = getConcretePaths(selectedFields);
        final RecordSchema schema = record.getSchema();
        paths.forEach(schema::removePath);
    }

    private List<RecordFieldRemovalPath> getConcretePaths(final List<FieldValue> selectedFields) {
        return selectedFields.stream().map(field -> {
            final RecordFieldRemovalPath path = new RecordFieldRemovalPath();
            addToPathIfNotRoot(field, path);

            Optional<FieldValue> parentOptional = field.getParent();
            while (parentOptional.isPresent()) {
                final FieldValue parent = parentOptional.get();
                addToPathIfNotRoot(parent, path);
                parentOptional = parent.getParent();
            }
            return path;
        }).collect(Collectors.toList());
    }

    private void addToPathIfNotRoot(final FieldValue field, final RecordFieldRemovalPath path) {
        field.getParent().ifPresent(parent -> path.add(field.getField().getFieldName()));
    }

    public static class RecordPathRemovalProperties {
        private static final Pattern ALL_ELEMENTS_REGEX = Pattern.compile("\\[\\s*(?:\\*|0\\s*\\.\\.\\s*-1)\\s*]$");
        private static final Pattern ARRAY_ELEMENTS_REGEX = Pattern.compile("\\[\\s*-?\\d+(?:\\s*,\\s*-?\\d+)*+\\s*]");
        private static final Pattern MAP_ELEMENTS_REGEX = Pattern.compile("\\[\\s*'[^']+'(?:\\s*,\\s*'[^']+')*+\\s*]");

        private final String recordPath;

        private final boolean appliedToAllElementsInCollection;
        private final boolean appliedToIndividualArrayElements;
        private final boolean appliedToIndividualMapElements;

        public RecordPathRemovalProperties(final String recordPath) {
            this.recordPath = recordPath;

            // ends with [*] or [0..-1]
            this.appliedToAllElementsInCollection = ALL_ELEMENTS_REGEX.matcher(recordPath).find();

            // contains an array reference [] with one or more element references, e.g. [1], [ 1, -1]
            this.appliedToIndividualArrayElements = ARRAY_ELEMENTS_REGEX.matcher(recordPath).find();

            // contains a map reference [] with one or more element references, e.g. ['one'], ['one' , 'two' ]
            this.appliedToIndividualMapElements = MAP_ELEMENTS_REGEX.matcher(recordPath).find();
        }

        String getRecordPath() {
            return recordPath;
        }

        boolean isAppliedToAllElementsInCollection() {
            return appliedToAllElementsInCollection;
        }

        boolean isAppliedToIndividualArrayElements() {
            return appliedToIndividualArrayElements;
        }

        boolean isAppliedToIndividualMapElements() {
            return appliedToIndividualMapElements;
        }

        boolean isRemovingFieldsNotJustElementsFromWithinCollection() {
            return !isAppliedToIndividualArrayElements() && !isAppliedToIndividualMapElements();
        }
    }
}

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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldRemovalPath;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RecordFieldRemover {
    private Record record;

    public RecordFieldRemover(Record record) {
        this.record = record;
    }

    public void remove(String path) {
        RecordPath recordPath = new RecordPath(path);
        RecordPathResult recordPathResult = org.apache.nifi.record.path.RecordPath.compile(recordPath.toString()).evaluate(record);
        List<FieldValue> selectedFields = recordPathResult.getSelectedFields().collect(Collectors.toList());

        if (recordPath.isAppliedToAllElementsInCollection()) {
            removeAllElementsFromCollection(selectedFields);
        } else {
            selectedFields.forEach(field -> field.remove());
        }

        if (recordPath.pathRemovalRequiresSchemaModification()) {
            modifySchema(selectedFields);
        }
    }

    public Record getRecord() {
        record.regenerateSchema();
        return record;
    }

    private void removeAllElementsFromCollection(List<FieldValue> selectedFields) {
        if (!selectedFields.isEmpty()) {
            Optional<FieldValue> parentOptional = selectedFields.get(0).getParent();
            if (parentOptional.isPresent()) {
                FieldValue parent = parentOptional.get();
                parent.removeContent();
            }
        }
    }

    private void modifySchema(List<FieldValue> selectedFields) {
        List<RecordFieldRemovalPath> concretePaths = getConcretePaths(selectedFields);
        removePathsFromSchema(concretePaths);
    }

    private List<RecordFieldRemovalPath> getConcretePaths(List<FieldValue> selectedFields) {
        List<RecordFieldRemovalPath> paths = new ArrayList<>(selectedFields.size());
        for (FieldValue field : selectedFields) {
            RecordFieldRemovalPath path = new RecordFieldRemovalPath();
            path.add(field.getField().getFieldName());

            Optional<FieldValue> parentOptional = field.getParent();
            while (parentOptional.isPresent()) {
                FieldValue parent = parentOptional.get();
                path.add(parent.getField().getFieldName());
                parentOptional = parent.getParent();
            }

            paths.add(path);
        }
        return paths;
    }

    private void removePathsFromSchema(List<RecordFieldRemovalPath> paths) {
        for (RecordFieldRemovalPath path : paths) {
            RecordSchema schema = record.getSchema();
            schema.removePath(path);
        }
    }

    private static class RecordPath {
        private String recordPath;

        public RecordPath(final String recordPath) {
            this.recordPath = preprocessRecordPath(recordPath);
        }

        public boolean isAppliedToAllElementsInCollection() {
            return recordPath.endsWith("[*]") || recordPath.endsWith("[0..-1]");
        }

        @Override
        public String toString() {
            return recordPath;
        }

        private String preprocessRecordPath(final String recordPath) {
            if (recordPath.endsWith("]")) {
                return unifyRecordPathEnd(recordPath);
            }
            return recordPath;
        }

        private String unifyRecordPathEnd(final String recordPath) {
            String lastSquareBracketsOperator = getLastSquareBracketsOperator(recordPath);
            if (lastSquareBracketsOperator.equals("[*]")) {
                return recordPath.substring(0, recordPath.lastIndexOf('[')) + "[*]";
            } else if (lastSquareBracketsOperator.equals("[0..-1]")) {
                return recordPath.substring(0, recordPath.lastIndexOf('[')) + "[0..-1]";
            } else {
                return recordPath;
            }
        }

        private String getLastSquareBracketsOperator(final String recordPath) {
            int beginIndex = recordPath.lastIndexOf('[');
            return recordPath.substring(beginIndex).replaceAll("\\s","");
        }

        public boolean pathRemovalRequiresSchemaModification() {
            return allSquareBracketsContainAsteriskOnly(recordPath);
        }

        private boolean allSquareBracketsContainAsteriskOnly(String recordPath) {
            boolean allSquareBracketsContainAsteriskOnly = true;
            boolean inSquareBrackets = false;
            for (int i = 0; i < recordPath.length() && allSquareBracketsContainAsteriskOnly; ++i) {
                char character = recordPath.charAt(i);
                if (inSquareBrackets) {
                    switch (character) {
                        case ' ':
                        case '*':
                            break;
                        case ']':
                            inSquareBrackets = false;
                            break;
                        default:
                            allSquareBracketsContainAsteriskOnly = false;
                    }
                } else {
                    if (character == '[') {
                        inSquareBrackets = true;
                    }
                }
            }
            return allSquareBracketsContainAsteriskOnly;
        }
    }
}

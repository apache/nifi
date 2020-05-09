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
package org.apache.nifi.record.path.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.nifi.record.path.ArrayIndexFieldValue;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.MapEntryFieldValue;

public class FieldValueLogicalPathBuilder {

    private static final CharSequence DEFAULT_DELIMITER = "/";
    private static final CharSequence DEFAULT_KEY_INDEX_WRAPPER_LEFT = "[";
    private static final CharSequence DEFAULT_KEY_INDEX_WRAPPER_RIGHT = "]";
    private final CharSequence pathDelimiter;
    private final CharSequence keyLeft;
    private final CharSequence keyRight;
    private final CharSequence indexLeft;
    private final CharSequence indexRight;

    public static class Builder {

        private CharSequence pathDelimiter = DEFAULT_DELIMITER;
        private CharSequence keyLeft = DEFAULT_KEY_INDEX_WRAPPER_LEFT;
        private CharSequence keyRight = DEFAULT_KEY_INDEX_WRAPPER_RIGHT;
        private CharSequence indexLeft = DEFAULT_KEY_INDEX_WRAPPER_LEFT;
        private CharSequence indexRight = DEFAULT_KEY_INDEX_WRAPPER_RIGHT;

        public Builder() {
        }

        public Builder withPathDelimiter(CharSequence delimiter) {
            Objects.requireNonNull(delimiter, "delimiter cannot be null");
            this.pathDelimiter = delimiter;
            return this;
        }

        public Builder withMapKeyWrapperLeft(CharSequence left) {
            Objects.requireNonNull(left, "left cannot be null");
            this.keyLeft = left;
            return this;
        }

        public Builder withMapKeyWrapperRight(CharSequence right) {
            Objects.requireNonNull(right, "right cannot be null");
            this.keyRight = right;
            return this;
        }

        public Builder withArrayIndexWrapperLeft(CharSequence left) {
            Objects.requireNonNull(left, "left cannot be null");
            this.indexLeft = left;
            return this;
        }

        public Builder withArrayIndexWrapperRight(CharSequence right) {
            Objects.requireNonNull(right, "right cannot be null");
            this.indexRight = right;
            return this;
        }

        public FieldValueLogicalPathBuilder build() {
            return new FieldValueLogicalPathBuilder(pathDelimiter, keyLeft, keyRight, indexLeft,
                indexRight);
        }
    }

    private FieldValueLogicalPathBuilder(CharSequence pathDelimiter,
        CharSequence leftMapKeyWrapper, CharSequence rightMapKeyMapper, CharSequence leftArrayIndexWrapper,
        CharSequence rightArrayIndexWrapper) {
        this.keyLeft = leftMapKeyWrapper;
        this.keyRight = rightMapKeyMapper;
        this.indexLeft = leftArrayIndexWrapper;
        this.indexRight = rightArrayIndexWrapper;
        this.pathDelimiter = pathDelimiter;
    }

    /**
     * Builds a logical path String using the configured wrappers for array or map values for a given
     * {@code FieldValue}
     *
     * @param fieldValue the Field Value
     * @return a String with a path
     */
    public String buildLogicalPath(FieldValue fieldValue) {
        Objects.requireNonNull(fieldValue, "fieldValue cannot be null");
        final List<CharSequence> paths = new ArrayList<>();
        FieldValueWalker.walk(fieldValue, (thisFieldValue) -> {
            int index = -1;
            if (thisFieldValue instanceof ArrayIndexFieldValue) {
                index = ((ArrayIndexFieldValue) thisFieldValue).getArrayIndex();
                paths.add(keyRight);
                paths.add(String.valueOf(index));
                paths.add(keyLeft);
            } else if (thisFieldValue instanceof MapEntryFieldValue) {
                paths.add(indexRight);
                paths.add(((MapEntryFieldValue) thisFieldValue).getMapKey());
                paths.add(indexLeft);
            } else {
                thisFieldValue.getParent().ifPresent((parentFieldValue) -> {
                    paths.add(thisFieldValue.getField().getFieldName());
                    paths.add(pathDelimiter);
                });
            }
        });
        Collections.reverse(paths);
        return String.join("",paths);
    }

}

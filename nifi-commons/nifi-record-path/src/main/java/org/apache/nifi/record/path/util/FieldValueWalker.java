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

import java.util.Objects;
import java.util.function.Consumer;

import org.apache.nifi.record.path.FieldValue;

/**
 * FieldValueWalker walks a FieldValue hierarchy from a given {@code FieldValue} to the root.
 * Each {@code FieldValue} in the hierarchy will be passed to a {@code Consumer}.
 */
public class FieldValueWalker {

    /**
     * Walk the hierarchy from a given {@code FieldValue} with the provided {@code Consumer}.
     *
     * @param fieldValue FieldValue to start with
     * @param consumer Consumer for each FieldValue
     */
    public static void walk(FieldValue fieldValue, Consumer<FieldValue> consumer) {
        Objects.requireNonNull(fieldValue, "fieldValue cannot be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        consumer.accept(fieldValue);
        fieldValue.getParent().ifPresent(value -> walk(value, consumer));
    }
}

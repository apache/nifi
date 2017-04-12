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

package org.apache.nifi.serialization;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.Assert;
import org.junit.Test;

public class TestSimpleRecordSchema {

    @Test
    public void testPreventsTwoFieldsWithSameAlias() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("goodbye", RecordFieldType.STRING.getDataType(), null, set("baz", "bar")));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with same alias");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testPreventsTwoFieldsWithSameName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType()));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with same name");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testPreventsTwoFieldsWithConflictingNamesAliases() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("bar", RecordFieldType.STRING.getDataType()));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with conflicting names/aliases");
        } catch (final IllegalArgumentException expected) {
        }
    }

    private Set<String> set(final String... values) {
        final Set<String> set = new HashSet<>();
        for (final String value : values) {
            set.add(value);
        }
        return set;
    }

}

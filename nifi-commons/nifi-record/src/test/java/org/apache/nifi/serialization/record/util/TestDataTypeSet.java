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

package org.apache.nifi.serialization.record.util;

import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDataTypeSet {

    @Test
    public void testCombineNarrowThenWider() {
        final DataTypeSet set = new DataTypeSet();
        set.add(RecordFieldType.INT.getDataType());
        set.add(RecordFieldType.DOUBLE.getDataType());
        assertEquals(Collections.singletonList(RecordFieldType.DOUBLE.getDataType()), set.getTypes());
    }

    @Test
    public void testAddIncompatible() {
        final DataTypeSet set = new DataTypeSet();
        set.add(RecordFieldType.INT.getDataType());
        set.add(RecordFieldType.BOOLEAN.getDataType());
        assertEquals(Arrays.asList(RecordFieldType.INT.getDataType(), RecordFieldType.BOOLEAN.getDataType()), set.getTypes());

    }

    @Test
    public void addSingleType() {
        final DataTypeSet set = new DataTypeSet();
        set.add(RecordFieldType.INT.getDataType());
        assertEquals(Collections.singletonList(RecordFieldType.INT.getDataType()), set.getTypes());

    }

    @Test
    public void testCombineWiderThenNarrow() {
        final DataTypeSet set = new DataTypeSet();
        set.add(RecordFieldType.DOUBLE.getDataType());
        set.add(RecordFieldType.INT.getDataType());
        assertEquals(Collections.singletonList(RecordFieldType.DOUBLE.getDataType()), set.getTypes());
    }

    @Test
    public void testAddNothing() {
        final DataTypeSet set = new DataTypeSet();
        assertEquals(Collections.emptyList(), set.getTypes());
    }
}

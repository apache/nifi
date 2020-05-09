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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.record.Record;
import org.junit.Test;

public class TestFieldValueWalker extends AbstractWalkerTest {

    @Test
    public void testWalk() {
        final Record record = buildDefaultRecord();

        List<FieldValue> fieldValues =
            RecordPath.compile("//intArray[0..-1][. = 1]").evaluate(record).getSelectedFields().collect(
                Collectors.toList());
        assertEquals(1, fieldValues.size());

        // root mapRecord intArray intArray
        testNodeDepth(fieldValues, 4);

        fieldValues =
            RecordPath.compile("/mapRecordArray[0..-1]/intArray[0..-1][. > 1]").evaluate(record).getSelectedFields()
                .collect(Collectors.toList());
        assertEquals(4, fieldValues.size());

        // root mapRecordArray mapRecordArray intArray intArray
        testNodeDepth(fieldValues, 5);
    }
}
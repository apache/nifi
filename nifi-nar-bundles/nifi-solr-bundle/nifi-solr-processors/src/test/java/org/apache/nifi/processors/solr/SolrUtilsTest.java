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
package org.apache.nifi.processors.solr;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class SolrUtilsTest {

    @Mock
    private SolrInputDocument inputDocument;

    @Test
    public void test() throws Exception {
        // given
        final String value = "12345678901234567890.123456789012345678901234567890";
        final BigDecimal bigDecimalValue = new BigDecimal(value);
        final List<RecordField> fields = Collections.singletonList(new RecordField("test", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));

        final Map<String, Object> values = new HashMap<>();
        values.put("test", bigDecimalValue);

        final Record record = new MapRecord(new SimpleRecordSchema(fields), values);
        final List<String> fieldsToIndex = Collections.singletonList("parent_test");

        // when
        SolrUtils.writeRecord(record, inputDocument, fieldsToIndex, "parent");

        // then
        Mockito.verify(inputDocument, Mockito.times(1)).addField("parent_test", bigDecimalValue);
    }
}
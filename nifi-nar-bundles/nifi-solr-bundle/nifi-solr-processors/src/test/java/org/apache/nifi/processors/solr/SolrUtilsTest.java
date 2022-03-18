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

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SolrUtilsTest {

    @Mock
    private SolrInputDocument inputDocument;

    @Mock
    private PropertyContext context;

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

    @Test
    public void testCreateSolrClientWithChrootSuffix() {
        when(context.getProperty(SolrUtils.SOLR_SOCKET_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));
        when(context.getProperty(SolrUtils.SOLR_CONNECTION_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));
        when(context.getProperty(SolrUtils.SOLR_MAX_CONNECTIONS)).thenReturn(new MockPropertyValue("5"));
        when(context.getProperty(SolrUtils.SOLR_MAX_CONNECTIONS_PER_HOST)).thenReturn(new MockPropertyValue("5"));
        when(context.getProperty(SolrUtils.SOLR_TYPE)).thenReturn(new MockPropertyValue(SolrUtils.SOLR_TYPE_CLOUD.getValue()));
        when(context.getProperty(SolrUtils.SSL_CONTEXT_SERVICE)).thenReturn(new MockPropertyValue(null));
        when(context.getProperty(SolrUtils.KERBEROS_CREDENTIALS_SERVICE)).thenReturn(new MockPropertyValue(null));
        when(context.getProperty(SolrUtils.KERBEROS_PRINCIPAL)).thenReturn(new MockPropertyValue("kerb_principal"));
        when(context.getProperty(SolrUtils.KERBEROS_PASSWORD)).thenReturn(new MockPropertyValue("kerb_password"));
        when(context.getProperty(SolrUtils.COLLECTION)).thenReturn(new MockPropertyValue("collection"));
        when(context.getProperty(SolrUtils.ZK_CLIENT_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));
        when(context.getProperty(SolrUtils.ZK_CONNECTION_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));

        final String solrLocation = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/solr/UAT/something";
        SolrClient testClient = SolrUtils.createSolrClient(context, solrLocation);
        assertNotNull(testClient);
    }

    @Test
    public void testCreateSolrClientWithoutChrootSuffix() {
        when(context.getProperty(SolrUtils.SOLR_SOCKET_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));
        when(context.getProperty(SolrUtils.SOLR_CONNECTION_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));
        when(context.getProperty(SolrUtils.SOLR_MAX_CONNECTIONS)).thenReturn(new MockPropertyValue("5"));
        when(context.getProperty(SolrUtils.SOLR_MAX_CONNECTIONS_PER_HOST)).thenReturn(new MockPropertyValue("5"));
        when(context.getProperty(SolrUtils.SOLR_TYPE)).thenReturn(new MockPropertyValue(SolrUtils.SOLR_TYPE_CLOUD.getValue()));
        when(context.getProperty(SolrUtils.SSL_CONTEXT_SERVICE)).thenReturn(new MockPropertyValue(null));
        when(context.getProperty(SolrUtils.KERBEROS_CREDENTIALS_SERVICE)).thenReturn(new MockPropertyValue(null));
        when(context.getProperty(SolrUtils.KERBEROS_PRINCIPAL)).thenReturn(new MockPropertyValue("kerb_principal"));
        when(context.getProperty(SolrUtils.KERBEROS_PASSWORD)).thenReturn(new MockPropertyValue("kerb_password"));
        when(context.getProperty(SolrUtils.COLLECTION)).thenReturn(new MockPropertyValue("collection"));
        when(context.getProperty(SolrUtils.ZK_CLIENT_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));
        when(context.getProperty(SolrUtils.ZK_CONNECTION_TIMEOUT)).thenReturn(new MockPropertyValue("2 s"));

        final String solrLocation = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181";
        SolrClient testClient = SolrUtils.createSolrClient(context, solrLocation);
        assertNotNull(testClient);
    }
}
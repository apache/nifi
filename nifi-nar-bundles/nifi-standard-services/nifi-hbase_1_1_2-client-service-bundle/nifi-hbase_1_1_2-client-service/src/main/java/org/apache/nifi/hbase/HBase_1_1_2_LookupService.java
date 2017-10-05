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

package org.apache.nifi.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;

@Tags({"hbase", "record", "lookup", "service"})
@CapabilityDescription(
    "A lookup service that retrieves one or more columns from HBase based on a supplied rowKey."
)
public class HBase_1_1_2_LookupService extends HBase_1_1_2_ClientService implements LookupService<Record> {
    private static final Set<String> REQUIRED_KEYS = Collections.singleton("rowKey");

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hb-lu-table-name")
            .displayName("Table Name")
            .description("The name of the table where look ups will be run.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor RETURN_CFS = new PropertyDescriptor.Builder()
            .name("hb-lu-return-cfs")
            .displayName("Column Families")
            .description("The column families that will be returned.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor RETURN_QFS = new PropertyDescriptor.Builder()
            .name("hb-lu-return-qfs")
            .displayName("Column Qualifiers")
            .description("The column qualifies that will be returned.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();
    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("hb-lu-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    private String tableName;
    private List<byte[]> families;
    private List<byte[]> qualifiers;
    private Charset charset;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        List<PropertyDescriptor> retVal = new ArrayList<>();
        retVal.add(TABLE_NAME);
        retVal.add(RETURN_CFS);
        retVal.add(RETURN_QFS);
        retVal.add(CHARSET);
        return retVal;
    }

    @Override
    public Optional<Record> lookup(Map<String, String> coordinates) throws LookupFailureException {
        byte[] rowKey = coordinates.get("rowKey").getBytes();
        try {
            Map<String, Object> values = new HashMap<>();
            try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
                Get get = new Get(rowKey);
                Result result = table.get(get);

                for (byte[] fam : families) {
                    NavigableMap<byte[], byte[]>  map = result.getFamilyMap(fam);
                    for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                        if (qualifiers.contains(entry.getKey()) || qualifiers.size() == 0) {
                            values.put(new String(entry.getKey(), charset), new String(entry.getValue(), charset));
                        }
                    }
                }
            }

            if (values.size() > 0) {
                final List<RecordField> fields = new ArrayList<>();
                for (String key : values.keySet()) {
                    fields.add(new RecordField(key, RecordFieldType.STRING.getDataType()));
                }
                final RecordSchema schema = new SimpleRecordSchema(fields);
                return Optional.ofNullable(new MapRecord(schema, values));
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            getLogger().error("Error occurred loading {}", new Object[] { coordinates.get("rowKey") }, e);
            throw new LookupFailureException(e);
        }
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {
        super.onEnabled(context);

        this.tableName = context.getProperty(TABLE_NAME).getValue();
        this.charset = Charset.forName(context.getProperty(CHARSET).getValue());

        String families = context.getProperty(RETURN_CFS).getValue();
        String[] familiesSplit = families.split(",");
        this.families = new ArrayList<>();
        for (String fs : familiesSplit) {
            this.families.add(fs.trim().getBytes());
        }
        this.qualifiers = new ArrayList<>();
        String quals = context.getProperty(RETURN_QFS).getValue();

        if (quals != null && quals.length() > 0) {
            String[] qualsSplit = quals.split(",");
            for (String q : qualsSplit) {
                this.qualifiers.add(q.trim().getBytes());
            }
        }
    }
}

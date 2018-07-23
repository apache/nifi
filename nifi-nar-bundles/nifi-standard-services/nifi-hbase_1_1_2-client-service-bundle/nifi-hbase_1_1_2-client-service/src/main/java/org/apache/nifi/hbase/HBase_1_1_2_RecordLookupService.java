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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
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
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.hbase.VisibilityLabelUtils.AUTHORIZATIONS;

@Tags({"hbase", "record", "lookup", "service"})
@CapabilityDescription("A lookup service that retrieves one or more columns from HBase and returns them as a record. The lookup coordinates " +
        "must contain 'rowKey' which will be the HBase row id.")
public class HBase_1_1_2_RecordLookupService extends AbstractControllerService implements LookupService<Record> {

    static final String ROW_KEY_KEY = "rowKey";
    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ROW_KEY_KEY)));

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("hbase-client-service")
            .displayName("HBase Client Service")
            .description("Specifies the HBase Client Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hb-lu-table-name")
            .displayName("Table Name")
            .description("The name of the table where look ups will be run.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor RETURN_COLUMNS = new PropertyDescriptor.Builder()
            .name("hb-lu-return-cols")
            .displayName("Columns")
            .description("A comma-separated list of \\\"<colFamily>:<colQualifier>\\\" pairs to return when scanning. " +
                    "To return all columns for a given family, leave off the qualifier such as \\\"<colFamily1>,<colFamily2>\\\".")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("hb-lu-charset")
            .displayName("Character Set")
            .description("Specifies the character set used to decode bytes retrieved from HBase.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final List<PropertyDescriptor> PROPERTIES;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HBASE_CLIENT_SERVICE);
        props.add(TABLE_NAME);
        props.add(AUTHORIZATIONS);
        props.add(RETURN_COLUMNS);
        props.add(CHARSET);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    private String tableName;
    private List<Column> columns;
    private Charset charset;
    private HBaseClientService hBaseClientService;
    private List<String> authorizations;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates.get(ROW_KEY_KEY) == null) {
            return Optional.empty();
        }

        final String rowKey = coordinates.get(ROW_KEY_KEY).toString();
        if (StringUtils.isBlank(rowKey)) {
            return Optional.empty();
        }

        final byte[] rowKeyBytes = rowKey.getBytes(StandardCharsets.UTF_8);
        try {
            final Map<String, Object> values = new HashMap<>();

            hBaseClientService.scan(tableName, rowKeyBytes, rowKeyBytes, columns, authorizations, (byte[] row, ResultCell[] resultCells) ->  {
                for (final ResultCell cell : resultCells) {
                    final byte[] qualifier = Arrays.copyOfRange(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierOffset() + cell.getQualifierLength());
                    final byte[] value = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength());
                    values.put(new String(qualifier, charset), new String(value, charset));
                }
            });

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
        this.hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
        this.tableName = context.getProperty(TABLE_NAME).getValue();
        this.columns = getColumns(context.getProperty(RETURN_COLUMNS).getValue());
        this.charset = Charset.forName(context.getProperty(CHARSET).getValue());
        this.authorizations = VisibilityLabelUtils.getAuthorizations(context);
    }

    @OnDisabled
    public void onDisabled() {
        this.hBaseClientService = null;
        this.tableName = null;
        this.columns = null;
        this.charset = null;
    }

    private List<Column> getColumns(final String columnsValue) {
        final String[] columns = (columnsValue == null || columnsValue.isEmpty() ? new String[0] : columnsValue.split(","));

        final List<Column> columnsList = new ArrayList<>();

        for (final String column : columns) {
            if (column.contains(":"))  {
                final String[] parts = column.trim().split(":");
                final byte[] cf = parts[0].getBytes(StandardCharsets.UTF_8);
                final byte[] cq = parts[1].getBytes(StandardCharsets.UTF_8);
                columnsList.add(new Column(cf, cq));
            } else {
                final byte[] cf = column.trim().getBytes(StandardCharsets.UTF_8);
                columnsList.add(new Column(cf, null));
            }
        }

        return columnsList;
    }

}


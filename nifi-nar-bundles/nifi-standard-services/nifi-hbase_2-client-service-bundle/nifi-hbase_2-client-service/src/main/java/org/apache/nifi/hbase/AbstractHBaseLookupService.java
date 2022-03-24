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

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

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
import java.util.Set;

import static org.apache.nifi.hbase.VisibilityLabelUtils.AUTHORIZATIONS;

public abstract class AbstractHBaseLookupService extends AbstractControllerService {
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

    static final String ROW_KEY_KEY = "rowKey";
    protected static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ROW_KEY_KEY)));

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

    protected String tableName;
    protected List<Column> columns;
    protected Charset charset;
    protected HBaseClientService hBaseClientService;
    protected List<String> authorizations;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
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

    protected List<Column> getColumns(final String columnsValue) {
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

    protected Map<String, Object> scan(byte[] rowKeyBytes) throws IOException {
        final Map<String, Object> values = new HashMap<>();

        hBaseClientService.scan(tableName, rowKeyBytes, rowKeyBytes, columns, authorizations, (byte[] row, ResultCell[] resultCells) ->  {
            for (final ResultCell cell : resultCells) {
                final byte[] qualifier = Arrays.copyOfRange(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierOffset() + cell.getQualifierLength());
                final byte[] value = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength());
                values.put(new String(qualifier, charset), new String(value, charset));
            }
        });

        return values;
    }


}

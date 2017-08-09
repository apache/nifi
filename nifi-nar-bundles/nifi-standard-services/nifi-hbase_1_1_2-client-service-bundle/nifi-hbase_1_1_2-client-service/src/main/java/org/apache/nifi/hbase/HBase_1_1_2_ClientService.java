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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.KerberosTicketRenewer;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@RequiresInstanceClassLoading
@Tags({ "hbase", "client"})
@CapabilityDescription("Implementation of HBaseClientService for HBase 1.1.2. This service can be configured by providing " +
        "a comma-separated list of configuration files, or by specifying values for the other properties. If configuration files " +
        "are provided, they will be loaded first, and the values of the additional properties will override the values from " +
        "the configuration files. In addition, any user defined properties on the processor will also be passed to the HBase " +
        "configuration.")
@DynamicProperty(name="The name of an HBase configuration property.", value="The value of the given HBase configuration property.",
        description="These properties will be set on the HBase configuration after loading any provided configuration files.")
public class HBase_1_1_2_ClientService extends AbstractControllerService implements HBaseClientService {

    static final String HBASE_CONF_ZK_QUORUM = "hbase.zookeeper.quorum";
    static final String HBASE_CONF_ZK_PORT = "hbase.zookeeper.property.clientPort";
    static final String HBASE_CONF_ZNODE_PARENT = "zookeeper.znode.parent";
    static final String HBASE_CONF_CLIENT_RETRIES = "hbase.client.retries.number";

    static final long TICKET_RENEWAL_PERIOD = 60000;

    private volatile Connection connection;
    private volatile UserGroupInformation ugi;
    private volatile KerberosTicketRenewer renewer;

    private List<PropertyDescriptor> properties;
    private KerberosProperties kerberosProperties;
    private volatile File kerberosConfigFile = null;

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        kerberosConfigFile = config.getKerberosConfigurationFile();
        kerberosProperties = getKerberosProperties(kerberosConfigFile);

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HADOOP_CONF_FILES);
        props.add(kerberosProperties.getKerberosPrincipal());
        props.add(kerberosProperties.getKerberosKeytab());
        props.add(ZOOKEEPER_QUORUM);
        props.add(ZOOKEEPER_CLIENT_PORT);
        props.add(ZOOKEEPER_ZNODE_PARENT);
        props.add(HBASE_CLIENT_RETRIES);
        props.add(PHOENIX_CLIENT_JAR_LOCATION);
        this.properties = Collections.unmodifiableList(props);
    }

    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return new KerberosProperties(kerberosConfigFile);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' in the HBase configuration.")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        boolean confFileProvided = validationContext.getProperty(HADOOP_CONF_FILES).isSet();
        boolean zkQuorumProvided = validationContext.getProperty(ZOOKEEPER_QUORUM).isSet();
        boolean zkPortProvided = validationContext.getProperty(ZOOKEEPER_CLIENT_PORT).isSet();
        boolean znodeParentProvided = validationContext.getProperty(ZOOKEEPER_ZNODE_PARENT).isSet();
        boolean retriesProvided = validationContext.getProperty(HBASE_CLIENT_RETRIES).isSet();

        final List<ValidationResult> problems = new ArrayList<>();

        if (!confFileProvided && (!zkQuorumProvided || !zkPortProvided || !znodeParentProvided || !retriesProvided)) {
            problems.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(this.getClass().getSimpleName())
                    .explanation("ZooKeeper Quorum, ZooKeeper Client Port, ZooKeeper ZNode Parent, and HBase Client Retries are required " +
                            "when Hadoop Configuration Files are not provided.")
                    .build());
        }

        if (confFileProvided) {
            final String configFiles = validationContext.getProperty(HADOOP_CONF_FILES).getValue();
            ValidationResources resources = validationResourceHolder.get();

            // if no resources in the holder, or if the holder has different resources loaded,
            // then load the Configuration and set the new resources in the holder
            if (resources == null || !configFiles.equals(resources.getConfigResources())) {
                getLogger().debug("Reloading validation resources");
                resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
                validationResourceHolder.set(resources);
            }

            final Configuration hbaseConfig = resources.getConfiguration();
            final String principal = validationContext.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
            final String keytab = validationContext.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();

            problems.addAll(KerberosProperties.validatePrincipalAndKeytab(
                    this.getClass().getSimpleName(), hbaseConfig, principal, keytab, getLogger()));
        }

        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {
        this.connection = createConnection(context);

        // connection check
        if (this.connection != null) {
            final Admin admin = this.connection.getAdmin();
            if (admin != null) {
                admin.listTableNames();
            }

            // if we got here then we have a successful connection, so if we have a ugi then start a renewer
            if (ugi != null) {
                final String id = getClass().getSimpleName();
                renewer = SecurityUtil.startTicketRenewalThread(id, ugi, TICKET_RENEWAL_PERIOD, getLogger());
            }
        }
    }

    protected Connection createConnection(final ConfigurationContext context) throws IOException, InterruptedException {
        final String configFiles = context.getProperty(HADOOP_CONF_FILES).getValue();
        final Configuration hbaseConfig = getConfigurationFromFiles(configFiles);

        // override with any properties that are provided
        if (context.getProperty(ZOOKEEPER_QUORUM).isSet()) {
            hbaseConfig.set(HBASE_CONF_ZK_QUORUM, context.getProperty(ZOOKEEPER_QUORUM).getValue());
        }
        if (context.getProperty(ZOOKEEPER_CLIENT_PORT).isSet()) {
            hbaseConfig.set(HBASE_CONF_ZK_PORT, context.getProperty(ZOOKEEPER_CLIENT_PORT).getValue());
        }
        if (context.getProperty(ZOOKEEPER_ZNODE_PARENT).isSet()) {
            hbaseConfig.set(HBASE_CONF_ZNODE_PARENT, context.getProperty(ZOOKEEPER_ZNODE_PARENT).getValue());
        }
        if (context.getProperty(HBASE_CLIENT_RETRIES).isSet()) {
            hbaseConfig.set(HBASE_CONF_CLIENT_RETRIES, context.getProperty(HBASE_CLIENT_RETRIES).getValue());
        }

        // add any dynamic properties to the HBase configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hbaseConfig.set(descriptor.getName(), entry.getValue());
            }
        }

        if (SecurityUtil.isSecurityEnabled(hbaseConfig)) {
            final String principal = context.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
            final String keyTab = context.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();

            getLogger().info("HBase Security Enabled, logging in as principal {} with keytab {}", new Object[] {principal, keyTab});
            ugi = SecurityUtil.loginKerberos(hbaseConfig, principal, keyTab);
            getLogger().info("Successfully logged in as principal {} with keytab {}", new Object[] {principal, keyTab});

            return ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    return ConnectionFactory.createConnection(hbaseConfig);
                }
            });

        } else {
            getLogger().info("Simple Authentication");
            return ConnectionFactory.createConnection(hbaseConfig);
        }

    }

    protected Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hbaseConfig = HBaseConfiguration.create();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hbaseConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hbaseConfig;
    }

    @OnDisabled
    public void shutdown() {
        if (renewer != null) {
            renewer.stop();
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (final IOException ioe) {
                getLogger().warn("Failed to close connection to HBase due to {}", new Object[]{ioe});
            }
        }
    }

    @Override
    public void put(final String tableName, final Collection<PutFlowFile> puts) throws IOException {
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            // Create one Put per row....
            final Map<String, Put> rowPuts = new HashMap<>();
            for (final PutFlowFile putFlowFile : puts) {
                //this is used for the map key as a byte[] does not work as a key.
                final String rowKeyString = new String(putFlowFile.getRow(), StandardCharsets.UTF_8);
                Put put = rowPuts.get(rowKeyString);
                if (put == null) {
                    put = new Put(putFlowFile.getRow());
                    rowPuts.put(rowKeyString, put);
                }

                for (final PutColumn column : putFlowFile.getColumns()) {
                    if (column.getTimestamp() != null) {
                        put.addColumn(
                                column.getColumnFamily(),
                                column.getColumnQualifier(),
                                column.getTimestamp(),
                                column.getBuffer());
                    } else {
                        put.addColumn(
                                column.getColumnFamily(),
                                column.getColumnQualifier(),
                                column.getBuffer());
                    }
                }
            }

            table.put(new ArrayList<>(rowPuts.values()));
        }
    }

    @Override
    public void put(final String tableName, final byte[] rowId, final Collection<PutColumn> columns) throws IOException {
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowId);
            for (final PutColumn column : columns) {
                put.addColumn(
                        column.getColumnFamily(),
                        column.getColumnQualifier(),
                        column.getBuffer());
            }
            table.put(put);
        }
    }

    @Override
    public boolean checkAndPut(final String tableName, final byte[] rowId, final byte[] family, final byte[] qualifier, final byte[] value, final PutColumn column) throws IOException {
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowId);
            put.addColumn(
                column.getColumnFamily(),
                column.getColumnQualifier(),
                column.getBuffer());
            return table.checkAndPut(rowId, family, qualifier, value, put);
        }
    }

    @Override
    public void delete(final String tableName, final byte[] rowId) throws IOException {
        try (final Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(rowId);
            table.delete(delete);
        }
    }

    @Override
    public void scan(final String tableName, final Collection<Column> columns, final String filterExpression, final long minTime, final ResultHandler handler)
            throws IOException {

        Filter filter = null;
        if (!StringUtils.isBlank(filterExpression)) {
            ParseFilter parseFilter = new ParseFilter();
            filter = parseFilter.parseFilterString(filterExpression);
        }

        try (final Table table = connection.getTable(TableName.valueOf(tableName));
             final ResultScanner scanner = getResults(table, columns, filter, minTime)) {

            for (final Result result : scanner) {
                final byte[] rowKey = result.getRow();
                final Cell[] cells = result.rawCells();

                if (cells == null) {
                    continue;
                }

                // convert HBase cells to NiFi cells
                final ResultCell[] resultCells = new ResultCell[cells.length];
                for (int i=0; i < cells.length; i++) {
                    final Cell cell = cells[i];
                    final ResultCell resultCell = getResultCell(cell);
                    resultCells[i] = resultCell;
                }

                // delegate to the handler
                handler.handle(rowKey, resultCells);
            }
        }
    }

    @Override
    public void scan(final String tableName, final byte[] startRow, final byte[] endRow, final Collection<Column> columns, final ResultHandler handler)
            throws IOException {

        try (final Table table = connection.getTable(TableName.valueOf(tableName));
             final ResultScanner scanner = getResults(table, startRow, endRow, columns)) {

            for (final Result result : scanner) {
                final byte[] rowKey = result.getRow();
                final Cell[] cells = result.rawCells();

                if (cells == null) {
                    continue;
                }

                // convert HBase cells to NiFi cells
                final ResultCell[] resultCells = new ResultCell[cells.length];
                for (int i=0; i < cells.length; i++) {
                    final Cell cell = cells[i];
                    final ResultCell resultCell = getResultCell(cell);
                    resultCells[i] = resultCell;
                }

                // delegate to the handler
                handler.handle(rowKey, resultCells);
            }
        }
    }

    // protected and extracted into separate method for testing
    protected ResultScanner getResults(final Table table, final byte[] startRow, final byte[] endRow, final Collection<Column> columns) throws IOException {
        final Scan scan = new Scan();
        scan.setStartRow(startRow);
        scan.setStopRow(endRow);

        if (columns != null) {
            for (Column col : columns) {
                if (col.getQualifier() == null) {
                    scan.addFamily(col.getFamily());
                } else {
                    scan.addColumn(col.getFamily(), col.getQualifier());
                }
            }
        }

        return table.getScanner(scan);
    }

    // protected and extracted into separate method for testing
    protected ResultScanner getResults(final Table table, final Collection<Column> columns, final Filter filter, final long minTime) throws IOException {
        // Create a new scan. We will set the min timerange as the latest timestamp that
        // we have seen so far. The minimum timestamp is inclusive, so we will get duplicates.
        // We will record any cells that have the latest timestamp, so that when we scan again,
        // we know to throw away those duplicates.
        final Scan scan = new Scan();
        scan.setTimeRange(minTime, Long.MAX_VALUE);

        if (filter != null) {
            scan.setFilter(filter);
        }

        if (columns != null) {
            for (Column col : columns) {
                if (col.getQualifier() == null) {
                    scan.addFamily(col.getFamily());
                } else {
                    scan.addColumn(col.getFamily(), col.getQualifier());
                }
            }
        }

        return table.getScanner(scan);
    }

    private ResultCell getResultCell(Cell cell) {
        final ResultCell resultCell = new ResultCell();
        resultCell.setRowArray(cell.getRowArray());
        resultCell.setRowOffset(cell.getRowOffset());
        resultCell.setRowLength(cell.getRowLength());

        resultCell.setFamilyArray(cell.getFamilyArray());
        resultCell.setFamilyOffset(cell.getFamilyOffset());
        resultCell.setFamilyLength(cell.getFamilyLength());

        resultCell.setQualifierArray(cell.getQualifierArray());
        resultCell.setQualifierOffset(cell.getQualifierOffset());
        resultCell.setQualifierLength(cell.getQualifierLength());

        resultCell.setTimestamp(cell.getTimestamp());
        resultCell.setTypeByte(cell.getTypeByte());
        resultCell.setSequenceId(cell.getSequenceId());

        resultCell.setValueArray(cell.getValueArray());
        resultCell.setValueOffset(cell.getValueOffset());
        resultCell.setValueLength(cell.getValueLength());

        resultCell.setTagsArray(cell.getTagsArray());
        resultCell.setTagsOffset(cell.getTagsOffset());
        resultCell.setTagsLength(cell.getTagsLength());
        return resultCell;
    }

    static protected class ValidationResources {
        private final String configResources;
        private final Configuration configuration;

        public ValidationResources(String configResources, Configuration configuration) {
            this.configResources = configResources;
            this.configuration = configuration;
        }

        public String getConfigResources() {
            return configResources;
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }

    @Override
    public byte[] toBytes(boolean b) {
        return Bytes.toBytes(b);
    }

    @Override
    public byte[] toBytes(float f) {
        return Bytes.toBytes(f);
    }

    @Override
    public byte[] toBytes(int i) {
        return Bytes.toBytes(i);
    }

    @Override
    public byte[] toBytes(long l) {
        return Bytes.toBytes(l);
    }

    @Override
    public byte[] toBytes(double d) {
        return Bytes.toBytes(d);
    }

    @Override
    public byte[] toBytes(String s) {
        return Bytes.toBytes(s);
    }

    @Override
    public byte[] toBytesBinary(String s) {
        return Bytes.toBytesBinary(s);
    }
}

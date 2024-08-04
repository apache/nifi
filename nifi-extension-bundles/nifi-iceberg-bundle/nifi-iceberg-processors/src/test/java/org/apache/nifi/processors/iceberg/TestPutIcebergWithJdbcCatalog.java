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
package org.apache.nifi.processors.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.processors.iceberg.catalog.IcebergJdbcClientPool;
import org.apache.nifi.processors.iceberg.util.IcebergTestUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.iceberg.JdbcCatalogService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.dbcp.utils.DBCPProperties.DATABASE_URL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVERNAME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_USER;
import static org.apache.nifi.processors.iceberg.PutIceberg.ICEBERG_RECORD_COUNT;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.CATALOG_NAME;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.CATALOG_SERVICE;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.createTemporaryDirectory;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.getSystemTemporaryDirectory;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validateData;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validateNumberOfDataFiles;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validatePartitionFolders;
import static org.apache.nifi.services.iceberg.JdbcCatalogService.CONNECTION_POOL;
import static org.apache.nifi.services.iceberg.JdbcCatalogService.WAREHOUSE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

@DisabledOnOs(WINDOWS)
public class TestPutIcebergWithJdbcCatalog extends AbstractTestPutIceberg {

    private static final String DERBY_LOG_PROPERTY = "derby.stream.error.file";
    private static final String CONNECTION_POOL_SERVICE = "connection-pool-service";

    private static DBCPConnectionPool connectionPool;

    @BeforeAll
    public static void initConnectionPool() {
        setDerbyLog();
        connectionPool = new DBCPConnectionPool();
    }

    @AfterAll
    public static void clearDerbyLog() {
        System.clearProperty(DERBY_LOG_PROPERTY);
    }

    private void initServices(PartitionSpec spec) throws InitializationException {
        initDBCPService();
        initRecordReader();
        initCatalogService();
        createTestTable(spec);
    }

    private void initCatalogService() throws InitializationException {
        final JdbcCatalogService catalogService = new JdbcCatalogService();
        runner.addControllerService(CATALOG_SERVICE, catalogService);
        runner.setProperty(catalogService, CONNECTION_POOL, CONNECTION_POOL_SERVICE);
        runner.setProperty(catalogService, WAREHOUSE_PATH, warehousePath);
        runner.enableControllerService(catalogService);
        runner.setProperty(PutIceberg.CATALOG, CATALOG_SERVICE);
    }

    private void initDBCPService() throws InitializationException {
        final String url = String.format("jdbc:derby:%s;create=true", createTemporaryDirectory());
        runner.addControllerService(CONNECTION_POOL_SERVICE, connectionPool);
        runner.setProperty(connectionPool, DATABASE_URL, url);
        runner.setProperty(connectionPool, DB_USER, String.class.getSimpleName());
        runner.setProperty(connectionPool, DB_PASSWORD, String.class.getName());
        runner.setProperty(connectionPool, DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.enableControllerService(connectionPool);
    }

    private void createTestTable(PartitionSpec spec) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);

        final Function<Map<String, String>, JdbcClientPool> clientPoolBuilder = props -> new IcebergJdbcClientPool(props, connectionPool);
        final Function<Map<String, String>, FileIO> ioBuilder = props -> CatalogUtil.loadFileIO("org.apache.iceberg.hadoop.HadoopFileIO", props, new Configuration());

        catalog = new JdbcCatalog(ioBuilder, clientPoolBuilder, true);
        catalog.initialize("jdbc-catalog", properties);

        final Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());

        catalog.createTable(TABLE_IDENTIFIER, USER_SCHEMA, spec, tableProperties);
    }

    public static void setDerbyLog() {
        final File derbyLog = new File(getSystemTemporaryDirectory(), "derby.log");
        derbyLog.deleteOnExit();
        System.setProperty(DERBY_LOG_PROPERTY, derbyLog.getAbsolutePath());
    }

    @Test
    public void onTriggerBucketPartitioned() throws Exception {
        final PartitionSpec spec = PartitionSpec.builderFor(USER_SCHEMA)
                .bucket("department", 3)
                .build();

        runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        initServices(spec);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.enqueue(new byte[0]);
        runner.run();

        final Table table = catalog.loadTable(TABLE_IDENTIFIER);

        final List<Record> expectedRecords = IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
                .add(0, "John", "Finance")
                .add(1, "Jill", "Finance")
                .add(2, "James", "Marketing")
                .add(3, "Joana", "Sales")
                .build();

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).getFirst();

        final String tableLocation = new URI(table.location()).getPath();
        assertTrue(table.spec().isPartitioned());
        assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateData(table, expectedRecords, 0);
        validateNumberOfDataFiles(tableLocation, 3);
        validatePartitionFolders(tableLocation, Arrays.asList(
                "department_bucket=0", "department_bucket=1", "department_bucket=2"));
    }
}

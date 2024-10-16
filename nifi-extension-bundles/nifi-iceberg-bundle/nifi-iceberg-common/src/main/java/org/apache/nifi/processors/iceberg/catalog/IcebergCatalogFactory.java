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
package org.apache.nifi.processors.iceberg.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.services.iceberg.IcebergCatalogProperty;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.processors.iceberg.IcebergUtils.getConfigurationFromFiles;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.CATALOG_NAME;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.FILE_IO_IMPLEMENTATION;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.CLIENT_POOL_SERVICE;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.METASTORE_URI;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.WAREHOUSE_LOCATION;

public class IcebergCatalogFactory {

    private final IcebergCatalogService catalogService;

    public IcebergCatalogFactory(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public BaseMetastoreCatalog create() {
        return switch (catalogService.getCatalogType()) {
            case HIVE -> initHiveCatalog(catalogService);
            case HADOOP -> initHadoopCatalog(catalogService);
            case JDBC -> initJdbcCatalog(catalogService);
        };
    }

    private BaseMetastoreCatalog initHiveCatalog(IcebergCatalogService catalogService) {
        HiveCatalog catalog = new HiveCatalog();

        if (catalogService.getConfigFilePaths() != null) {
            final Configuration configuration = getConfigurationFromFiles(catalogService.getConfigFilePaths());
            catalog.setConf(configuration);
        }

        final Map<IcebergCatalogProperty, Object> catalogProperties = catalogService.getCatalogProperties();
        final Map<String, String> properties = new HashMap<>();

        if (catalogProperties.containsKey(METASTORE_URI)) {
            properties.put(CatalogProperties.URI, (String) catalogProperties.get(METASTORE_URI));
        }

        if (catalogProperties.containsKey(WAREHOUSE_LOCATION)) {
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, (String) catalogProperties.get(WAREHOUSE_LOCATION));
        }

        catalog.initialize("hive-catalog", properties);
        return catalog;
    }

    private BaseMetastoreCatalog initHadoopCatalog(IcebergCatalogService catalogService) {
        final Map<IcebergCatalogProperty, Object> catalogProperties = catalogService.getCatalogProperties();
        final String warehousePath = (String) catalogProperties.get(WAREHOUSE_LOCATION);

        if (catalogService.getConfigFilePaths() != null) {
            return new HadoopCatalog(getConfigurationFromFiles(catalogService.getConfigFilePaths()), warehousePath);
        } else {
            return new HadoopCatalog(new Configuration(), warehousePath);
        }
    }

    private BaseMetastoreCatalog initJdbcCatalog(IcebergCatalogService catalogService) {
        final Map<IcebergCatalogProperty, Object> catalogProperties = catalogService.getCatalogProperties();
        final Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, (String) catalogProperties.get(WAREHOUSE_LOCATION));

        final Configuration configuration = getConfigurationFromFiles(catalogService.getConfigFilePaths());
        final DBCPService dbcpService = (DBCPService) catalogProperties.get(CLIENT_POOL_SERVICE);

        final Function<Map<String, String>, JdbcClientPool> clientPoolBuilder = props -> new IcebergJdbcClientPool(props, dbcpService);
        final Function<Map<String, String>, FileIO> ioBuilder = props -> CatalogUtil.loadFileIO((String) catalogProperties.get(FILE_IO_IMPLEMENTATION), props, configuration);

        JdbcCatalog catalog = new JdbcCatalog(ioBuilder, clientPoolBuilder, false);
        catalog.setConf(configuration);
        catalog.initialize((String) catalogProperties.get(CATALOG_NAME), properties);
        return catalog;
    }
}

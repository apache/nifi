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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.nifi.services.iceberg.IcebergCatalogProperties;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.iceberg.IcebergUtils.getConfigurationFromFiles;

public class IcebergCatalogFactory {

    private final IcebergCatalogService catalogService;

    public IcebergCatalogFactory(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public Catalog create() {
        return switch (catalogService.getCatalogServiceType()) {
            case HiveCatalogService -> initHiveCatalog(catalogService);
            case HadoopCatalogService -> initHadoopCatalog(catalogService);
            default -> throw new IllegalArgumentException("Unknown catalog type: " + catalogService.getCatalogServiceType());
        };
    }

    private Catalog initHiveCatalog(IcebergCatalogService catalogService) {
        HiveCatalog catalog = new HiveCatalog();

        if (catalogService.getConfigFiles() != null) {
            final Configuration configuration = getConfigurationFromFiles(catalogService.getConfigFiles());
            catalog.setConf(configuration);
        }

        final Map<String, String> additionalParameters = catalogService.getAdditionalParameters();
        final Map <String, String> properties = new HashMap<>();

        if (additionalParameters.containsKey(IcebergCatalogProperties.METASTORE_URI)) {
            properties.put(CatalogProperties.URI, additionalParameters.get(IcebergCatalogProperties.METASTORE_URI));
        }

        if (additionalParameters.containsKey(IcebergCatalogProperties.WAREHOUSE_LOCATION)) {
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, additionalParameters.get(IcebergCatalogProperties.WAREHOUSE_LOCATION));
        }

        catalog.initialize("hive-catalog", properties);
        return catalog;
    }

    private Catalog initHadoopCatalog(IcebergCatalogService catalogService) {
        final Map<String, String> properties = catalogService.getAdditionalParameters();
        final String warehousePath = properties.get(IcebergCatalogProperties.WAREHOUSE_LOCATION);

        if (catalogService.getConfigFiles() != null) {
            return new HadoopCatalog(getConfigurationFromFiles(catalogService.getConfigFiles()), warehousePath);
        } else {
            return new HadoopCatalog(new Configuration(), warehousePath);
        }
    }
}

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
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.util.HashMap;
import java.util.Map;

public class TestHiveCatalogService extends AbstractControllerService implements IcebergCatalogService {

    private final HiveCatalog catalog;

    public TestHiveCatalogService(HiveCatalog catalog) {
        this.catalog = catalog;
    }

    public static class Builder {
        private String metastoreUri;
        private String warehouseLocation;
        private Configuration config;

        public Builder withMetastoreUri(String metastoreUri) {
            this.metastoreUri = metastoreUri;
            return this;
        }

        public Builder withWarehouseLocation(String warehouseLocation) {
            this.warehouseLocation = warehouseLocation;
            return this;
        }

        public Builder withConfig(Configuration config) {
            this.config = config;
            return this;
        }

        public TestHiveCatalogService build() {
            HiveCatalog catalog = new HiveCatalog();
            Map<String, String> properties = new HashMap<>();

            if (metastoreUri != null) {
                properties.put(CatalogProperties.URI, metastoreUri);
            }

            if (warehouseLocation != null) {
                properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
            }

            if (config != null) {
                catalog.setConf(config);
            }

            catalog.initialize("hive-catalog", properties);
            return new TestHiveCatalogService(catalog);
        }
    }

    @Override
    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public Configuration getConfiguration() {
        return catalog.getConf();
    }

}

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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.services.iceberg.IcebergCatalogProperty;
import org.apache.nifi.services.iceberg.IcebergCatalogService;
import org.apache.nifi.services.iceberg.IcebergCatalogType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.METASTORE_URI;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.WAREHOUSE_LOCATION;

public class TestHiveCatalogService extends AbstractControllerService implements IcebergCatalogService {

    private final List<String> configFilePaths;
    private final Map<IcebergCatalogProperty, String> catalogProperties;

    public TestHiveCatalogService(Map<IcebergCatalogProperty, String> catalogProperties, List<String> configFilePaths) {
        this.catalogProperties = catalogProperties;
        this.configFilePaths = configFilePaths;
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HIVE;
    }

    @Override
    public Map<IcebergCatalogProperty, String> getCatalogProperties() {
        return catalogProperties;
    }

    @Override
    public List<String> getConfigFilePaths() {
        return configFilePaths;
    }

    public static class Builder {
        private String metastoreUri;
        private String warehouseLocation;
        private List<String> configFilePaths;

        public Builder withMetastoreUri(String metastoreUri) {
            this.metastoreUri = metastoreUri;
            return this;
        }

        public Builder withWarehouseLocation(String warehouseLocation) {
            this.warehouseLocation = warehouseLocation;
            return this;
        }

        public Builder withConfigFilePaths(List<String> configFilePaths) {
            this.configFilePaths = configFilePaths;
            return this;
        }

        public TestHiveCatalogService build() {
            Map<IcebergCatalogProperty, String> properties = new HashMap<>();

            if (metastoreUri != null) {
                properties.put(METASTORE_URI, metastoreUri);
            }

            if (warehouseLocation != null) {
                properties.put(WAREHOUSE_LOCATION, warehouseLocation);
            }

            return new TestHiveCatalogService(properties, configFilePaths);
        }
    }
}

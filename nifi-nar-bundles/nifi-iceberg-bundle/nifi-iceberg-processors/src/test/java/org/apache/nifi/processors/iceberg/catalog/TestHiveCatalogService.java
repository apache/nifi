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
import org.apache.nifi.services.iceberg.IcebergCatalogProperties;
import org.apache.nifi.services.iceberg.IcebergCatalogService;
import org.apache.nifi.services.iceberg.IcebergCatalogServiceType;

import java.util.HashMap;
import java.util.Map;

public class TestHiveCatalogService extends AbstractControllerService implements IcebergCatalogService {

    private final String configFiles;
    private final Map<String, String> additionalParameters;

    public TestHiveCatalogService(Map<String, String> additionalParameters, String configFiles) {
        this.additionalParameters = additionalParameters;
        this.configFiles = configFiles;
    }

    @Override
    public IcebergCatalogServiceType getCatalogServiceType() {
        return IcebergCatalogServiceType.HiveCatalogService;
    }

    @Override
    public Map<String, String> getAdditionalParameters() {
        return additionalParameters;
    }

    @Override
    public String getConfigFiles() {
        return configFiles;
    }

    public static class Builder {
        private String metastoreUri;
        private String warehouseLocation;
        private String configFiles;

        public Builder withMetastoreUri(String metastoreUri) {
            this.metastoreUri = metastoreUri;
            return this;
        }

        public Builder withWarehouseLocation(String warehouseLocation) {
            this.warehouseLocation = warehouseLocation;
            return this;
        }

        public Builder withConfig(String configFiles) {
            this.configFiles = configFiles;
            return this;
        }

        public TestHiveCatalogService build() {
            Map<String, String> properties = new HashMap<>();

            if (metastoreUri != null) {
                properties.put(IcebergCatalogProperties.METASTORE_URI, metastoreUri);
            }

            if (warehouseLocation != null) {
                properties.put(IcebergCatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
            }

            return new TestHiveCatalogService(properties, configFiles);
        }
    }
}

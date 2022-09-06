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

    private Catalog catalog;

    public TestHiveCatalogService(String metastoreUri, String warehouseLocation) {
        initCatalog(metastoreUri, warehouseLocation);
    }

    public void initCatalog(String metastoreUri, String warehouseLocation) {
        catalog = new HiveCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, metastoreUri);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        catalog.initialize("hive-catalog", properties);
    }

    @Override
    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

}

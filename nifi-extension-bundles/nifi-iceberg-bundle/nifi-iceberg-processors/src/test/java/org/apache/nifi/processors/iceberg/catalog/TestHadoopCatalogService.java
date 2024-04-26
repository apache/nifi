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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.createTempDirectory;

public class TestHadoopCatalogService extends AbstractControllerService implements IcebergCatalogService {

    private final Map<IcebergCatalogProperty, String> catalogProperties = new HashMap<>();

    public TestHadoopCatalogService() throws IOException {
        File warehouseLocation = createTempDirectory("metastore").toFile();
        catalogProperties.put(IcebergCatalogProperty.WAREHOUSE_LOCATION, warehouseLocation.getAbsolutePath());
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HADOOP;
    }

    @Override
    public Map<IcebergCatalogProperty, String> getCatalogProperties() {
        return catalogProperties;
    }

    @Override
    public List<String> getConfigFilePaths() {
        return null;
    }
}

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
package org.apache.nifi.services.iceberg;

public enum IcebergCatalogProperty {

    CATALOG_NAME,
    METASTORE_URI("hive.metastore.uris"),
    WAREHOUSE_LOCATION("hive.metastore.warehouse.dir"),
    CLIENT_POOL_SERVICE,
    CLIENT_POOL_SIZE,
    FILE_IO_IMPLEMENTATION;

    private static final String EMPTY_STRING = "";

    private final String hadoopPropertyName;

    IcebergCatalogProperty() {
        this.hadoopPropertyName = EMPTY_STRING;
    }

    IcebergCatalogProperty(String hadoopPropertyName) {
        this.hadoopPropertyName = hadoopPropertyName;
    }

    public String getHadoopPropertyName() {
        return hadoopPropertyName;
    }

}

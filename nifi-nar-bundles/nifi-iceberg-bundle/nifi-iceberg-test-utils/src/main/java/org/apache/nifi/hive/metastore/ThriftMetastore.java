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
package org.apache.nifi.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

/** A JUnit Extension that creates a Hive Metastore Thrift service backed by a Hive Metastore using an in-memory Derby database. */
public class ThriftMetastore implements BeforeAllCallback, AfterAllCallback {

    private final MetastoreCore metastoreCore;

    private Map<String, String> configOverrides = new HashMap<>();

    public ThriftMetastore() {
        metastoreCore = new MetastoreCore();
    }

    public ThriftMetastore withConfigOverrides(Map<String, String> configs) {
        configOverrides = configs;
        return this;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        metastoreCore.initialize(configOverrides);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        metastoreCore.shutdown();
    }

    public String getThriftConnectionUri() {
        return metastoreCore.getThriftConnectionUri();
    }

    public String getWarehouseLocation() {
        return metastoreCore.getWarehouseLocation();
    }

    public HiveMetaStoreClient getMetaStoreClient() {
        return metastoreCore.getMetaStoreClient();
    }

    public Configuration getConfiguration() {
        return metastoreCore.getConfiguration();
    }

    public String getConfigurationLocation() {
        return metastoreCore.getConfigurationLocation();
    }

}

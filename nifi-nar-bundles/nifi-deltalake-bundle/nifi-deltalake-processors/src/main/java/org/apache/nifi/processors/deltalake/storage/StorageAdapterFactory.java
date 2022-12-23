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
package org.apache.nifi.processors.deltalake.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.processor.ProcessContext;

import java.lang.reflect.InvocationTargetException;

import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.HADOOP_CONFIGURATION_RESOURCES;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.STORAGE_SELECTOR;

public class StorageAdapterFactory {

    public StorageAdapter initializeAdapter(ProcessContext processorContext) {

        String location = processorContext.getProperty(STORAGE_SELECTOR).getValue();
        DeltaLakeStorageLocation storageLocation = DeltaLakeStorageLocation.valueOf(location);
        Configuration configuration = createConfiguration(processorContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).getValue());

        try {
            StorageAdapter storageAdapter = storageLocation.getServiceClass()
                    .getConstructor(ProcessContext.class, String.class, Configuration.class)
                    .newInstance(processorContext, storageLocation.engineInfo, configuration);
            return storageAdapter;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Error during StorageAdapter creation: " + storageLocation, e);
        }

    }

    private static Configuration createConfiguration(String hadoopConfigurationFiles) {
        Configuration configuration = new Configuration();
        if (hadoopConfigurationFiles != null) {
            for (final String configFile : hadoopConfigurationFiles.split(",")) {
                configuration.addResource(new Path(configFile.trim()));
            }
        }
        return configuration;
    }

}

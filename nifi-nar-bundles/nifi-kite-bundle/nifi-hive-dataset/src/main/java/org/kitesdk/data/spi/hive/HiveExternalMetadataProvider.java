/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.kitesdk.data.spi.hive;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.filesystem.FileSystemUtil;
import org.kitesdk.data.spi.filesystem.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveExternalMetadataProvider extends HiveAbstractMetadataProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveExternalMetadataProvider.class);
  private final Path rootDirectory;
  private final FileSystem rootFileSystem;

  public HiveExternalMetadataProvider(Configuration conf, Path rootDirectory) {
    super(conf);
    Preconditions.checkNotNull(rootDirectory, "Root cannot be null");

    try {
      this.rootFileSystem = rootDirectory.getFileSystem(conf);
      this.rootDirectory = rootFileSystem.makeQualified(rootDirectory);
    } catch (IOException ex) {
      throw new DatasetIOException("Could not get FileSystem for root path", ex);
    }
  }

  @Override
  public DatasetDescriptor create(String namespace, String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    String resolved = resolveNamespace(
        namespace, name, descriptor.getLocation());
    if (resolved != null) {
      if (resolved.equals(namespace)) {
        // the requested dataset already exists
        throw new DatasetExistsException(
            "Metadata already exists for dataset: " + namespace + "." + name);
      } else {
        // replacing old default.name table
        LOG.warn("Creating table {}.{} for {}: replaces default.{}",
            new Object[]{
                namespace, name, pathForDataset(namespace, name), name});
        // validate that the new metadata can read the existing data
        Compatibility.checkUpdate(load(resolved, name), descriptor);
      }
    }

    LOG.info("Creating an external Hive table: {}.{}", namespace, name);

    DatasetDescriptor newDescriptor = descriptor;

    if (descriptor.getLocation() == null) {
      // create a new descriptor with the dataset's location
      newDescriptor = new DatasetDescriptor.Builder(descriptor)
          .location(pathForDataset(namespace, name))
          .build();
    }

    Path managerPath = new Path(new Path(newDescriptor.getLocation().toString()),
        SCHEMA_DIRECTORY);

    // Store the schema with the schema manager and use the
    // managed URI moving forward.
    SchemaManager manager = SchemaManager.create(conf, managerPath);

    URI managedSchemaUri = manager.writeSchema(descriptor.getSchema());

    try {
      newDescriptor = new DatasetDescriptor.Builder(newDescriptor)
          .schemaUri(managedSchemaUri)
          .build();
    } catch (IOException e) {
      throw new DatasetIOException("Unable to load schema", e);
    }

    // create the data directory first so it is owned by the current user, not Hive
    FileSystemUtil.ensureLocationExists(newDescriptor, conf);

    // this object will be the table metadata
    Table table = HiveUtils.tableForDescriptor(
        namespace, name, newDescriptor, true /* external table */);

    // assign the location of the the table
    getMetaStoreUtil().createTable(table);

    return newDescriptor;
  }

  @Override
  protected URI expectedLocation(String namespace, String name) {
    return pathForDataset(namespace, name).toUri();
  }

  private Path pathForDataset(String namespace, String name) {
    Preconditions.checkState(rootDirectory != null,
        "Dataset repository root directory can not be null");

    return rootFileSystem.makeQualified(
        HiveUtils.pathForDataset(rootDirectory, namespace, name));
  }

  @Override
  public void partitionAdded(String namespace, String name, String path) {
    Path partitionPath = new Path(pathForDataset(namespace, name), path);
    try {
      rootFileSystem.mkdirs(partitionPath);
    } catch (IOException ex) {
      throw new DatasetIOException(
        "Unable to create partition directory  " + partitionPath, ex);
    }
    super.partitionAdded(namespace, name, path);
  }

}

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.PartitionListener;
import org.kitesdk.data.spi.filesystem.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HiveAbstractMetadataProvider extends AbstractMetadataProvider implements
    PartitionListener {

  static final String SCHEMA_DIRECTORY = ".metadata/schemas";

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAbstractMetadataProvider.class);

  protected final Configuration conf;
  private MetaStoreUtil metastore;

  HiveAbstractMetadataProvider(Configuration conf) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    this.conf = conf;
  }

  protected MetaStoreUtil getMetaStoreUtil() {
    if (metastore == null) {
      metastore = MetaStoreUtil.get(conf);
    }
    return metastore;
  }

  protected abstract URI expectedLocation(String namespace, String name);

  /**
   * Returns whether the table is a managed hive table.
   * @param name a Table name
   * @return true if the table is managed, false otherwise
   * @throws DatasetNotFoundException If the table does not exist in Hive
   */
  protected boolean isManaged(String namespace, String name) {
    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return isManaged(getMetaStoreUtil().getTable(resolved, name));
    }
    return false;
  }

  /**
   * Returns whether the table is a managed hive table.
   * @param name a Table name
   * @return true if the table is managed, false otherwise
   * @throws DatasetNotFoundException If the table does not exist in Hive
   */
  protected boolean isExternal(String namespace, String name) {
    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return isExternal(getMetaStoreUtil().getTable(resolved, name));
    }
    return false;
  }

  @Override
  public DatasetDescriptor load(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);

    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return HiveUtils.descriptorForTable(
          conf, getMetaStoreUtil().getTable(resolved, name));
    }
    throw new DatasetNotFoundException(
        "Hive table not found: " + namespace + "." + name);
  }

  @Override
  public DatasetDescriptor update(String namespace, String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      Table table = getMetaStoreUtil().getTable(resolved, name);

      Path managerPath = new Path(new Path(table.getSd().getLocation()),
          SCHEMA_DIRECTORY);

      SchemaManager manager = SchemaManager.create(conf, managerPath);

      DatasetDescriptor newDescriptor;

      try {
        URI schemaURI = manager.writeSchema(descriptor.getSchema());

        newDescriptor = new DatasetDescriptor.Builder(descriptor)
            .schemaUri(schemaURI).build();

      } catch (IOException e) {
        throw new DatasetIOException("Unable to create schema", e);
      }

      HiveUtils.updateTableSchema(table, newDescriptor);
      getMetaStoreUtil().alterTable(table);
      return descriptor;
    }
    throw new DatasetNotFoundException(
        "Hive table not found: " + namespace + "." + name);
  }

  @Override
  public boolean delete(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);
    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      getMetaStoreUtil().dropTable(resolved, name);
      return true;
    }
    return false;
  }

  @Override
  public boolean exists(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);
    return (resolveNamespace(namespace, name) != null);
  }

  @Override
  public Collection<String> namespaces() {
    Collection<String> databases = getMetaStoreUtil().getAllDatabases();
    List<String> databasesWithDatasets = Lists.newArrayList();
    for (String db : databases) {
      if (isNamespace(db)) {
        databasesWithDatasets.add(db);
      }
    }
    return databasesWithDatasets;
  }

  @Override
  public Collection<String> datasets(String namespace) {
    Collection<String> tables = getMetaStoreUtil().getAllTables(namespace);
    List<String> readableTables = Lists.newArrayList();
    for (String name : tables) {
      if (isReadable(namespace, name)) {
        readableTables.add(name);
      }
    }
    return readableTables;
  }

  /**
   * Returns true if there is at least one table in the give database that can
   * be read.
   *
   * @param database a Hive database name
   * @return {@code true} if there is at least one readable table in database
   * @see {@link #isReadable(String, String)}
   */
  private boolean isNamespace(String database) {
    Collection<String> tables = getMetaStoreUtil().getAllTables(database);
    for (String name : tables) {
      if (isReadable(database, name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the given table exists and can be read by this library.
   *
   * @param namespace a Hive database name
   * @param name a table name
   * @return {@code true} if the table exists and is supported
   */
  private boolean isReadable(String namespace, String name) {
    Table table = getMetaStoreUtil().getTable(namespace, name);
    if (isManaged(table) || isExternal(table)) { // readable table types
      try {
        // get a descriptor for the table. if this succeeds, it is readable
        HiveUtils.descriptorForTable(conf, table);
        return true;
      } catch (DatasetException e) {
        // not a readable table
      } catch (IllegalStateException e) {
        // not a readable table
      } catch (IllegalArgumentException e) {
        // not a readable table
      } catch (UnsupportedOperationException e) {
        // not a readable table
      }
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void partitionAdded(String namespace, String name, String path) {
    getMetaStoreUtil().addPartition(namespace, name, path);
  }


  @Override
  @SuppressWarnings("unchecked")
  public void partitionDeleted(String namespace, String name, String path) {

    getMetaStoreUtil().dropPartition(namespace, name, path);
  }

  /**
   * Checks whether the Hive table {@code namespace.name} exists or if
   * {@code default.name} exists and should be used.
   *
   * @param namespace the requested namespace
   * @param name the table name
   * @return if namespace.name exists, namespace. if not and default.name
   *          exists, then default. {@code null} otherwise.
   */
  protected String resolveNamespace(String namespace, String name) {
    return resolveNamespace(namespace, name, null);
  }

  /**
   * Checks whether the Hive table {@code namespace.name} exists or if
   * {@code default.name} exists and should be used.
   *
   * @param namespace the requested namespace
   * @param name the table name
   * @param location location that should match or null to check the default
   * @return if namespace.name exists, namespace. if not and default.name
   *          exists, then default. {@code null} otherwise.
   */
  protected String resolveNamespace(String namespace, String name,
                                    @Nullable URI location) {
    if (getMetaStoreUtil().exists(namespace, name)) {
      return namespace;
    }
    try {
      DatasetDescriptor descriptor = HiveUtils.descriptorForTable(
          conf, getMetaStoreUtil().getTable(URIBuilder.NAMESPACE_DEFAULT, name));
      URI expectedLocation = location;
      if (location == null) {
        expectedLocation = expectedLocation(namespace, name);
      }
      if ((expectedLocation == null) ||
          pathsEquivalent(expectedLocation, descriptor.getLocation())) {
        // table in the default db has the location that would have been used
        return URIBuilder.NAMESPACE_DEFAULT;
      }
      // fall through and return null
    } catch (DatasetNotFoundException e) {
      // fall through and return null
    }
    return null;
  }

  private static boolean pathsEquivalent(URI left, @Nullable URI right) {
    if (right == null) {
      return false;
    }
    String leftAuth = left.getAuthority();
    String rightAuth = right.getAuthority();
    if (leftAuth != null && rightAuth != null && !leftAuth.equals(rightAuth)) {
      // but authority sections are set, but do not match
      return false;
    }
    return (Objects.equal(left.getScheme(), right.getScheme()) &&
        Objects.equal(left.getPath(), right.getPath()));
  }

  @VisibleForTesting
  static boolean isManaged(Table table) {
    return TableType.MANAGED_TABLE.toString().equals(table.getTableType());
  }

  @VisibleForTesting
  static boolean isExternal(Table table) {
    return TableType.EXTERNAL_TABLE.toString().equals(table.getTableType());
  }

}

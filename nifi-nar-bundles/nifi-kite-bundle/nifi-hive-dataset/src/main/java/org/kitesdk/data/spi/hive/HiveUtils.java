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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;

class HiveUtils {
  static final String HDFS_SCHEME = "hdfs";

  private static final String CUSTOM_PROPERTIES_PROPERTY_NAME = "kite.custom.property.names";
  private static final String PARTITION_EXPRESSION_PROPERTY_NAME = "kite.partition.expression";
  private static final String COMPRESSION_TYPE_PROPERTY_NAME = "kite.compression.type";
  private static final String OLD_CUSTOM_PROPERTIES_PROPERTY_NAME = "cdk.custom.property.names";
  private static final String OLD_PARTITION_EXPRESSION_PROPERTY_NAME = "cdk.partition.expression";
  private static final String AVRO_SCHEMA_URL_PROPERTY_NAME = "avro.schema.url";
  private static final String AVRO_SCHEMA_LITERAL_PROPERTY_NAME = "avro.schema.literal";

  private static final Splitter NAME_SPLITTER = Splitter.on(',');
  private static final Joiner NAME_JOINER = Joiner.on(',');

  private static final Map<Format, String> FORMAT_TO_SERDE = ImmutableMap
      .<Format, String>builder()
      .put(Formats.AVRO, "org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .put(Formats.PARQUET, getHiveParquetSerde())
      .build();
  private static final Map<String, Format> SERDE_TO_FORMAT = ImmutableMap
      .<String, Format>builder()
      .put("org.apache.hadoop.hive.serde2.avro.AvroSerDe", Formats.AVRO)
      .put("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
           Formats.PARQUET)
      .put("parquet.hive.serde.ParquetHiveSerDe", Formats.PARQUET)
      .build();
  private static final Map<Format, String> FORMAT_TO_INPUT_FORMAT = ImmutableMap
      .<Format, String>builder()
      .put(Formats.AVRO,
           "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .put(Formats.PARQUET, getHiveParquetInputFormat())
      .build();
  private static final Map<Format, String> FORMAT_TO_OUTPUT_FORMAT = ImmutableMap
      .<Format, String>builder()
      .put(Formats.AVRO,
           "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .put(Formats.PARQUET, getHiveParquetOutputFormat())
      .build();

  static DatasetDescriptor descriptorForTable(Configuration conf, Table table) {
    final DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();

    Format format;
    final String serializationLib = table.getSd().getSerdeInfo().getSerializationLib();
    if (SERDE_TO_FORMAT.containsKey(serializationLib)) {
      format = SERDE_TO_FORMAT.get(serializationLib);
      builder.format(format);
    } else {
      // TODO: should this use an "unknown" format? others fail in open()
      throw new UnknownFormatException(
          "Unknown format for serde:" + serializationLib);
    }

    final Path dataLocation = new Path(table.getSd().getLocation());
    final FileSystem fs = fsForPath(conf, dataLocation);

    builder.location(fs.makeQualified(dataLocation));

    // custom properties
    Map<String, String> properties = table.getParameters();
    String namesProperty = coalesce(
        properties.get(CUSTOM_PROPERTIES_PROPERTY_NAME),
        properties.get(OLD_CUSTOM_PROPERTIES_PROPERTY_NAME));
    if (namesProperty != null) {
      for (String property : NAME_SPLITTER.split(namesProperty)) {
        builder.property(property, properties.get(property));
      }
    }

    PartitionStrategy partitionStrategy = null;
    if (isPartitioned(table)) {
      String partitionProperty = coalesce(
          properties.get(PARTITION_EXPRESSION_PROPERTY_NAME),
          properties.get(OLD_PARTITION_EXPRESSION_PROPERTY_NAME));
      if (partitionProperty != null) {
        partitionStrategy = Accessor.getDefault()
            .fromExpression(partitionProperty);
      } else {
        // build a partition strategy for the table from the Hive strategy
        partitionStrategy = fromPartitionColumns(getPartCols(table));
      }
      builder.partitionStrategy(partitionStrategy);
    }

    String schemaUrlString = properties.get(AVRO_SCHEMA_URL_PROPERTY_NAME);
    if (schemaUrlString != null) {
      try {
        // URI.create is safe because this library wrote the URI
        builder.schemaUri(URI.create(schemaUrlString));
      } catch (IOException e) {
        throw new DatasetIOException("Could not read schema", e);
      }
    } else {
      String schemaLiteral = properties.get(AVRO_SCHEMA_LITERAL_PROPERTY_NAME);
      if (schemaLiteral != null) {
        builder.schemaLiteral(schemaLiteral);
      } else {
        builder.schema(HiveSchemaConverter.convertTable(
            table.getTableName(), table.getSd().getCols(),
            partitionStrategy));
      }
    }

    String compressionType = properties.get(COMPRESSION_TYPE_PROPERTY_NAME);
    if (compressionType != null) {
      builder.compressionType(compressionType);
    }

    try {
      return builder.build();
    } catch (IllegalStateException ex) {
      throw new DatasetException("Cannot find schema: missing metadata");
    }
  }

  private static boolean isPartitioned(Table table) {
    return (getPartCols(table).size() != 0);
  }

  private static List<FieldSchema> getPartCols(Table table) {
    List<FieldSchema> partKeys = table.getPartitionKeys();
    if (partKeys == null) {
      partKeys = new ArrayList<FieldSchema>();
      table.setPartitionKeys(partKeys);
    }
    return partKeys;
  }

  /**
   * Returns the first non-null value from the sequence or null if there is no
   * non-null value.
   */
  private static <T> T coalesce(T... values) {
    for (T value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  static Table tableForDescriptor(String namespace, String name,
                                  DatasetDescriptor descriptor,
                                  boolean external) {

    return tableForDescriptor(namespace, name, descriptor, external, true);
  }

  static Table tableForDescriptor(String namespace, String name,
                                  DatasetDescriptor descriptor,
                                  boolean external,
                                  boolean includeSchema) {
    final Table table = createEmptyTable(namespace, name);

    if (external) {
      // you'd think this would do it...
      table.setTableType(TableType.EXTERNAL_TABLE.toString());
      // but it doesn't work without some additional magic:
      table.getParameters().put("EXTERNAL", "TRUE");
      table.getSd().setLocation(descriptor.getLocation().toString());
    } else {
      table.setTableType(TableType.MANAGED_TABLE.toString());
    }

    addPropertiesForDescriptor(table, descriptor);

    // translate from Format to SerDe
    final Format format = descriptor.getFormat();
    if (FORMAT_TO_SERDE.containsKey(format)) {
      table.getSd().getSerdeInfo().setSerializationLib(FORMAT_TO_SERDE.get(format));
      table.getSd().setInputFormat(FORMAT_TO_INPUT_FORMAT.get(format));
      table.getSd().setOutputFormat(FORMAT_TO_OUTPUT_FORMAT.get(format));
    } else {
      throw new UnknownFormatException(
          "No known serde for format:" + format.getName());
    }

    if (includeSchema) {
      URL schemaURL = descriptor.getSchemaUrl();
      if (useSchemaURL(schemaURL)) {
        table.getParameters().put(
            AVRO_SCHEMA_URL_PROPERTY_NAME,
            descriptor.getSchemaUrl().toExternalForm());
      } else {
        table.getParameters().put(
            AVRO_SCHEMA_LITERAL_PROPERTY_NAME,
            descriptor.getSchema().toString());
      }
    }

    table.getParameters().put(COMPRESSION_TYPE_PROPERTY_NAME,
        descriptor.getCompressionType().getName());

    // convert the schema to Hive columns
    table.getSd().setCols(HiveSchemaConverter.convertSchema(descriptor.getSchema()));

    // copy partitioning info
    if (descriptor.isPartitioned()) {
      PartitionStrategy ps = descriptor.getPartitionStrategy();
      table.getParameters().put(PARTITION_EXPRESSION_PROPERTY_NAME,
          Accessor.getDefault().toExpression(ps));
      table.setPartitionKeys(partitionColumns(ps, descriptor.getSchema()));
    }

    return table;
  }

  private static boolean useSchemaURL(@Nullable URL schemaURL) {
    try {
      return ((schemaURL != null) &&
          HDFS_SCHEME.equals(schemaURL.toURI().getScheme()));
    } catch (URISyntaxException ex) {
      return false;
    }
  }

  static Table createEmptyTable(String namespace, String name) {
    Table table = new Table();
    table.setDbName(namespace);
    table.setTableName(name);
    table.setPartitionKeys(new ArrayList<FieldSchema>());
    table.setParameters(new HashMap<String, String>());

    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<String>());
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setParameters(new HashMap<String, String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    SkewedInfo skewInfo = new SkewedInfo();
    skewInfo.setSkewedColNames(new ArrayList<String>());
    skewInfo.setSkewedColValues(new ArrayList<List<String>>());
    skewInfo.setSkewedColValueLocationMaps(new HashMap<List<String>, String>());
    sd.setSkewedInfo(skewInfo);
    table.setSd(sd);

    return table;
  }

  public static void updateTableSchema(Table table, DatasetDescriptor descriptor) {
    URL schemaURL = descriptor.getSchemaUrl();

    if (table.getParameters().get(AVRO_SCHEMA_LITERAL_PROPERTY_NAME) != null) {
      if (useSchemaURL(schemaURL)) {
        table.getParameters().remove(AVRO_SCHEMA_LITERAL_PROPERTY_NAME);
        table.getParameters().put(AVRO_SCHEMA_URL_PROPERTY_NAME,
            schemaURL.toExternalForm());
      } else {
        table.getParameters().put(
            AVRO_SCHEMA_LITERAL_PROPERTY_NAME,
            descriptor.getSchema().toString());
      }

    } else if (table.getParameters().get(AVRO_SCHEMA_URL_PROPERTY_NAME) != null) {
      if (schemaURL == null) {
        throw new DatasetOperationException(
            "Cannot update " + AVRO_SCHEMA_URL_PROPERTY_NAME +
            " since descriptor schema URL is not set.");
      }
      table.getParameters().put(
          AVRO_SCHEMA_URL_PROPERTY_NAME,
          schemaURL.toExternalForm());

    } else {
      // neither the literal or the URL are set, so add the URL if specified
      // and the schema literal if not.
      if (useSchemaURL(schemaURL)) {
        table.getParameters().put(
                AVRO_SCHEMA_URL_PROPERTY_NAME,
                schemaURL.toExternalForm());

      } else if (descriptor.getSchema() != null) {
        table.getParameters().put(
                AVRO_SCHEMA_LITERAL_PROPERTY_NAME,
                descriptor.getSchema().toString());
      } else {
        throw new DatasetException("Table schema cannot be updated since it is" +
                " not set on the descriptor.");
      }
    }

    // copy partitioning info
    if (descriptor.isPartitioned()) {
      PartitionStrategy ps = descriptor.getPartitionStrategy();
      table.getParameters().put(PARTITION_EXPRESSION_PROPERTY_NAME,
          Accessor.getDefault().toExpression(ps));
      // no need to set the partition columns; no changes to the Hive side
    }

    // keep the custom properties up-to-date
    addPropertiesForDescriptor(table, descriptor);

    // keep the table DDL up to-to-date with the Schema
    table.getSd().setCols(
        HiveSchemaConverter.convertSchema(descriptor.getSchema()));
  }

  static FileSystem fsForPath(Configuration conf, Path path) {
    try {
      return path.getFileSystem(conf);
    } catch (IOException ex) {
      throw new DatasetIOException("Cannot access FileSystem for uri:" + path, ex);
    }
  }

  private static void addPropertiesForDescriptor(Table table,
                                                 DatasetDescriptor descriptor) {
    // copy custom properties to the table
    if (!descriptor.listProperties().isEmpty()) {
      for (String property : descriptor.listProperties()) {
        // no need to check the reserved list, those are not set on descriptors
        table.getParameters().put(property, descriptor.getProperty(property));
      }
      // set which properties are custom and should be set on descriptors
      table.getParameters().put(CUSTOM_PROPERTIES_PROPERTY_NAME,
          NAME_JOINER.join(descriptor.listProperties()));
    }
  }

  /**
   * Returns the correct dataset path for the given name and root directory.
   *
   * @param root A Path
   * @param namespace A String namespace, or logical group
   * @param name A String dataset name
   * @return the correct dataset Path
   */
  static Path pathForDataset(Path root, String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    // Why replace '.' here? Is this a namespacing hack?
    return new Path(root, new Path(namespace, name.replace('.', Path.SEPARATOR_CHAR)));
  }

  @SuppressWarnings("deprecation")
  static List<FieldSchema> partitionColumns(PartitionStrategy strategy, Schema schema) {
    List<FieldSchema> columns = Lists.newArrayList();
    for (FieldPartitioner<?, ?> fp : Accessor.getDefault().getFieldPartitioners(strategy)) {
      columns.add(new FieldSchema(fp.getName(),
          getHiveType(SchemaUtil.getPartitionType(fp, schema)),
          "Partition column derived from '" + fp.getSourceName() + "' column, " +
              "generated by Kite."));
    }
    return columns;
  }

  private static final Map<String, String> PROVIDED_TYPES = ImmutableMap
      .<String, String>builder()
      .put("tinyint", "int")
      .put("smallint", "int")
      .put("int", "int")
      .put("bigint", "long")
      .build();

  /**
   * Builds a {@link PartitionStrategy} from a list of Hive partition fields.
   *
   * @param fields a List of FieldSchemas
   * @return a PartitionStrategy for the Hive partitions
   */
  @VisibleForTesting
  static PartitionStrategy fromPartitionColumns(List<FieldSchema> fields) {
    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();
    for (FieldSchema hiveSchema : fields) {
      TypeInfo type = HiveSchemaConverter.parseTypeInfo(hiveSchema.getType());
      // any types not in the map will be treated as Strings
      builder.provided(hiveSchema.getName(),
          PROVIDED_TYPES.get(type.getTypeName()));
    }
    return builder.build();
  }

  private static String getHiveType(Class<?> type) {
    String typeName = PrimitiveObjectInspectorUtils.getTypeNameFromPrimitiveJava(type);
    if (typeName == null) {
      throw new DatasetException("Unsupported FieldPartitioner type: " + type);
    }
    return typeName;
  }

  private static String getHiveParquetInputFormat() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    String oldClass = "parquet.hive.DeprecatedParquetInputFormat";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }

  private static String getHiveParquetOutputFormat() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    String oldClass = "parquet.hive.DeprecatedParquetOutputFormat";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (java.lang.NoClassDefFoundError ex) {
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }

  private static String getHiveParquetSerde() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    String oldClass = "parquet.hive.serde.ParquetHiveSerDe";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (java.lang.NoClassDefFoundError ex) {
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }
}

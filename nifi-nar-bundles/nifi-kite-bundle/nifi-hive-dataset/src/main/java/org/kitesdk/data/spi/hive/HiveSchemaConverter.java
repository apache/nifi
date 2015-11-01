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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.codehaus.jackson.node.NullNode;
import org.kitesdk.compat.DynConstructors;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;

public class HiveSchemaConverter {

  private static final DynMethods.StaticMethod primitiveTypeForName =
      new DynMethods.Builder("getPrimitiveTypeInfo")
          .impl(TypeInfoFactory.class, String.class)
          .buildStatic();

  private static final DynMethods.StaticMethod parseTypeInfo =
      new DynMethods.Builder("getTypeInfoFromTypeString")
          .impl(TypeInfoUtils.class, String.class)
          .buildStatic();

  private static Class<?> findTypeInfoClass(String className) {
    try {
      return new DynConstructors.Builder(TypeInfo.class)
          .impl(className).buildChecked().getConstructedClass();
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  // TypeInfo classes that may not be present at runtime
  @VisibleForTesting
  static final Class<?> charClass = findTypeInfoClass(
      "org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo");
  @VisibleForTesting
  static final Class<?> varcharClass = findTypeInfoClass(
      "org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo");
  @VisibleForTesting
  static final Class<?> decimalClass = findTypeInfoClass(
      "org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo");

  private static final ImmutableMap<String, Schema.Type> TYPEINFO_TO_TYPE =
      ImmutableMap.<String, Schema.Type>builder()
          .put("boolean", Schema.Type.BOOLEAN)
          .put("tinyint", Schema.Type.INT)
          .put("smallint", Schema.Type.INT)
          .put("int", Schema.Type.INT)
          .put("bigint", Schema.Type.LONG)
          .put("float", Schema.Type.FLOAT)
          .put("double", Schema.Type.DOUBLE)
          .put("string", Schema.Type.STRING)
          .put("binary", Schema.Type.BYTES)
          .put("void", Schema.Type.NULL) // void columns are placeholders
          .build();

  static final ImmutableMap<Schema.Type, TypeInfo> TYPE_TO_TYPEINFO =
      ImmutableMap.<Schema.Type, TypeInfo>builder()
          .put(Schema.Type.BOOLEAN, primitiveTypeInfo("boolean"))
          .put(Schema.Type.INT, primitiveTypeInfo("int"))
          .put(Schema.Type.LONG, primitiveTypeInfo("bigint"))
          .put(Schema.Type.FLOAT, primitiveTypeInfo("float"))
          .put(Schema.Type.DOUBLE, primitiveTypeInfo("double"))
          .put(Schema.Type.STRING, primitiveTypeInfo("string"))
          .put(Schema.Type.ENUM, primitiveTypeInfo("string"))
          .put(Schema.Type.BYTES, primitiveTypeInfo("binary"))
          .put(Schema.Type.FIXED, primitiveTypeInfo("binary"))
          .build();

  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  @VisibleForTesting
  static final NullNode NULL_DEFAULT = NullNode.getInstance();
  @VisibleForTesting
  static final Collection<String[]> NO_REQUIRED_FIELDS = ImmutableList.of();

  private static TypeInfo primitiveTypeInfo(String type) {
    return primitiveTypeForName.invoke(type);
  }

  public static TypeInfo parseTypeInfo(String type) {
    return parseTypeInfo.invoke(type);
  }

  public static Schema convertTable(String table, Collection<FieldSchema> columns,
                                    @Nullable PartitionStrategy strategy) {
    ArrayList<String> fieldNames = Lists.newArrayList();
    ArrayList<TypeInfo> fieldTypes = Lists.newArrayList();
    LinkedList<String> start = Lists.newLinkedList();
    Collection<String[]> requiredFields = requiredFields(strategy);

    List<Schema.Field> fields = Lists.newArrayList();
    for (FieldSchema column : columns) {
      // pass null for the initial path to exclude the table name
      TypeInfo type = parseTypeInfo(column.getType());
      fieldNames.add(column.getName());
      fieldTypes.add(type);
      fields.add(convertField(start, column.getName(), type, requiredFields));
    }

    StructTypeInfo struct = new StructTypeInfo();
    struct.setAllStructFieldNames(fieldNames);
    struct.setAllStructFieldTypeInfos(fieldTypes);

    Schema recordSchema = Schema.createRecord(table, doc(struct), null, false);
    recordSchema.setFields(fields);

    return recordSchema;
  }

  private static Schema convert(LinkedList<String> path, String name,
                                StructTypeInfo type,
                                Collection<String[]> required) {
    List<String> names = type.getAllStructFieldNames();
    List<TypeInfo> types = type.getAllStructFieldTypeInfos();
    Preconditions.checkArgument(names.size() == types.size(),
        "Cannot convert struct: %s names != %s types",
        names.size(), types.size());

    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 0; i < names.size(); i += 1) {
      path.addLast(name);
      fields.add(convertField(path, names.get(i), types.get(i), required));
      path.removeLast();
    }

    Schema recordSchema = Schema.createRecord(name, doc(type), null, false);
    recordSchema.setFields(fields);

    return recordSchema;
  }

  private static Schema.Field convertField(LinkedList<String> path, String name,
                                           TypeInfo type,
                                           Collection<String[]> required) {
    // filter the required fields with the current name
    Collection<String[]> matchingRequired = filterByStartsWith(required, path, name);

    Schema schema = convert(path, name, type, matchingRequired);
    boolean isOptional = (schema.getType() == Schema.Type.UNION);

    if (!isOptional && matchingRequired.size() < 1) {
      // not already an optional union and not required, make it optional.
      // this doesn't complain if a required field is already optional because
      // the minimum required fields are validated by DatasetDescriptor.
      schema  = optional(schema);
      isOptional = true;
    }

    return new Schema.Field(name, schema, doc(type),
        isOptional ? NULL_DEFAULT : null);
  }

  @VisibleForTesting
  static Schema convert(LinkedList<String> path, String name,
                        TypeInfo type, Collection<String[]> required) {
    switch (type.getCategory()) {
      case PRIMITIVE:
        if (type.getClass() == charClass || type.getClass() == varcharClass) {
          // this is required because type name includes length
          return Schema.create(Schema.Type.STRING);
        }

        String typeInfoName = type.getTypeName();
        Preconditions.checkArgument(TYPEINFO_TO_TYPE.containsKey(typeInfoName),
            "Cannot convert unsupported type: %s", typeInfoName);
        return Schema.create(TYPEINFO_TO_TYPE.get(typeInfoName));

      case LIST:
        return Schema.createArray(optional(convert(path, name,
            ((ListTypeInfo) type).getListElementTypeInfo(), required)));

      case MAP:
        MapTypeInfo mapType = (MapTypeInfo) type;
        Preconditions.checkArgument(
            "string".equals(mapType.getMapKeyTypeInfo().toString()),
            "Non-String map key type: %s", mapType.getMapKeyTypeInfo());

        return Schema.createMap(optional(convert(path, name,
            mapType.getMapValueTypeInfo(), required)));

      case STRUCT:
        return convert(path, name, (StructTypeInfo) type, required);

      case UNION:
        List<TypeInfo> unionTypes = ((UnionTypeInfo) type)
            .getAllUnionObjectTypeInfos();

        // add NULL so all union types are optional
        List<Schema> types = Lists.newArrayList(NULL);
        for (int i = 0; i < unionTypes.size(); i += 1) {
          // types within unions cannot be required
          types.add(convert(
              path, name + "_" + i, unionTypes.get(i), NO_REQUIRED_FIELDS));
        }

        return Schema.createUnion(types);

      default:
        throw new IllegalArgumentException(
            "Unknown TypeInfo category: " + type.getCategory());
    }
  }

  @VisibleForTesting
  static Schema optional(Schema schema) {
    return Schema.createUnion(Lists.newArrayList(NULL, schema));
  }

  private static String doc(TypeInfo type) {
    if (type instanceof StructTypeInfo) {
      // don't add struct<a:t1,b:t2> when fields a and b will have doc strings
      return null;
    }
    return "Converted from '" + String.valueOf(type) + "'";
  }

  public static List<FieldSchema> convertSchema(Schema avroSchema) {
    List<FieldSchema> columns = Lists.newArrayList();
    if (Schema.Type.RECORD.equals(avroSchema.getType())) {
      for (Schema.Field field : avroSchema.getFields()) {
        columns.add(new FieldSchema(
            field.name(), convert(field.schema()).getTypeName(), field.doc()));
      }
    } else {
      columns.add(new FieldSchema(
          "column", convert(avroSchema).getTypeName(), avroSchema.getDoc()));
    }
    return columns;
  }

  @VisibleForTesting
  static TypeInfo convert(Schema schema) {
    return SchemaUtil.visit(schema, new Converter());
  }

  private static class Converter extends SchemaUtil.SchemaVisitor<TypeInfo> {
    public TypeInfo record(Schema record, List<String> names, List<TypeInfo> types) {
      return TypeInfoFactory.getStructTypeInfo(names, types);
    }

    @Override
    public TypeInfo union(Schema union, List<TypeInfo> options) {
      // so no need to keep track of whether the avro type is nullable because
      // all Hive types are nullable
      List<TypeInfo> nonNullTypes = Lists.newArrayList();
      for (TypeInfo type : options) {
        if (type != null) {
          nonNullTypes.add(type);
        }
      }

      // handle a single field in the union
      if (nonNullTypes.size() == 1) {
        return nonNullTypes.get(0);
      }

      return TypeInfoFactory.getUnionTypeInfo(nonNullTypes);
    }

    @Override
    public TypeInfo array(Schema array, TypeInfo element) {
      return TypeInfoFactory.getListTypeInfo(element);
    }

    @Override
    public TypeInfo map(Schema map, TypeInfo value) {
      return TypeInfoFactory.getMapTypeInfo(
          TYPE_TO_TYPEINFO.get(Schema.Type.STRING), value);
    }

    @Override
    public TypeInfo primitive(Schema primitive) {
      return TYPE_TO_TYPEINFO.get(primitive.getType());
    }
  }

  private static Collection<String[]> filterByStartsWith(
      Collection<String[]> fields, LinkedList<String> path, String name) {
    path.addLast(name);

    List<String[]> startsWithCollection = Lists.newArrayList();
    for (String[] field : fields) {
      if (startsWith(field, path)) {
        startsWithCollection.add(field);
      }
    }

    path.removeLast();

    return startsWithCollection;
  }

  /**
   * Returns true if left starts with right.
   */
  private static boolean startsWith(String[] left, List<String> right) {
    // short circuit if a match isn't possible
    if (left.length < right.size()) {
      return false;
    }

    for (int i = 0; i < right.size(); i += 1) {
      if (!left[i].equals(right.get(i))) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings("deprecation")
  private static Collection<String[]> requiredFields(@Nullable PartitionStrategy strategy) {
    if (strategy == null) {
      return NO_REQUIRED_FIELDS;
    }

    List<String[]> requiredFields = Lists.newArrayList();
    for (FieldPartitioner fp : Accessor.getDefault().getFieldPartitioners(strategy)) {
      // source name is not present for provided partitioners
      if (fp.getSourceName() != null) {
        requiredFields.add(fp.getSourceName().split("\\."));
      }
    }

    return requiredFields;
  }
}

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
package org.apache.nifi.schemaregistry.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({ "schema", "registry", "avro", "json", "csv" })
@CapabilityDescription("Provides a service for registering and accessing schemas. You can register schema "
        + "as a dynamic property where 'name' represents the schema name and 'value' represents the textual "
        + "representation of the actual schema.")
public class SimpleKeyValueSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        propertyDescriptors = Collections.emptyList();
    }

    private final Map<String, String> schemaNameToSchemaMap;

    public SimpleKeyValueSchemaRegistry() {
        this.schemaNameToSchemaMap = new HashMap<>();
    }

    @OnEnabled
    public void enable(ConfigurationContext configuratiponContext) throws InitializationException {
        this.schemaNameToSchemaMap.putAll(configuratiponContext.getProperties().entrySet().stream()
                .filter(propEntry -> propEntry.getKey().isDynamic())
                .collect(Collectors.toMap(propEntry -> propEntry.getKey().getName(), propEntry -> propEntry.getValue())));
    }

    /**
     *
     */
    @Override
    public String retrieveSchemaText(String schemaName) {
        if (!this.schemaNameToSchemaMap.containsKey(schemaName)) {
            throw new IllegalArgumentException("Failed to find schema; Name: '" + schemaName + ".");
        } else {
            return this.schemaNameToSchemaMap.get(schemaName);
        }
    }

    @Override
    public String retrieveSchemaText(String schemaName, Map<String, String> attributes) {
        throw new UnsupportedOperationException("This version of schema registry does not "
                + "support this operation, since schemas are only identofied by name.");
    }

    @Override
    @OnDisabled
    public void close() throws Exception {
        this.schemaNameToSchemaMap.clear();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().required(false).name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true).expressionLanguageSupported(true)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

	@Override
	public RecordSchema retrieveSchema(String schemaName) {
		String schemaText = this.retrieveSchemaText(schemaName);
		Schema schema = new Schema.Parser().parse(schemaText);
		
		List<RecordField> recordFields = new ArrayList<>();
		Type type = null;
		for (Field field : schema.getFields()) {		
			if (Type.UNION == field.schema().getType()){		
				type = this.extractUnionType(field);
			} else {
				type = field.schema().getType();
			}
			DataType dataType = this.toDataType(type);
			RecordField recordField = new RecordField(field.name(), dataType);
			recordFields.add(recordField);
		}
		
		SimpleRecordSchema recordSchema = new SimpleRecordSchema(recordFields);
		return recordSchema;
	}

	/*
	 * For this implementation 'attributes' argument is ignored since the underlying storage mechanisms 
	 * is based strictly on key/value pairs. In other implementation additional attributes may play a role (e.g., version id,) 
	 */
	@Override
	public RecordSchema retrieveSchema(String schemaName, Map<String, String> attributes) {
		return this.retrieveSchema(schemaName);
	}
	
	/*
	 * Will extract the concrete type from the Avro UNION type IF such type 
	 * is a primitive type (i.e., int, boolean, string etc.). This implies that 
	 * it does not currently support unions that contain complex types and 
	 * hierarchies of types.
	 * For any type that is not primitive this operation will throw 
	 * IllegalArgumentException
	 */   
	private Type extractUnionType(Field field){
		Type type = null;
		for (Schema t : field.schema().getTypes()) {
			type = t.getType();
			if (type != Type.NULL){
				if (type == Type.BOOLEAN || 
					type == Type.STRING ||
					type == Type.BYTES ||
					type == Type.FLOAT ||
					type == Type.INT ||
					type == Type.LONG){
					break;
				} else {
					throw new IllegalArgumentException("Non-primitive types are not currently supported in UNION");
				}
			} // skip otherwise
		}
		return type;
	}
	
	/*
	 * Converts from Avro type to RecordFieldType type.
	 */
	private DataType toDataType(Type type) {
		if (type == Type.BOOLEAN){
			return RecordFieldType.BOOLEAN.getDataType();
		} else if (type == Type.STRING){
			return RecordFieldType.STRING.getDataType();
		} else if (type == Type.BYTES){
			return RecordFieldType.BYTE.getDataType();
		} else if (type == Type.FLOAT){
			return RecordFieldType.FLOAT.getDataType();
		} else if (type == Type.INT){
			return RecordFieldType.INT.getDataType();
		} else if (type == Type.LONG){
			return RecordFieldType.LONG.getDataType();
		} else {
			throw new IllegalArgumentException("Unsupported type"); // should never happen.
		}
	}
}

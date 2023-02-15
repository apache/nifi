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
package org.apache.nifi.processors.salesforce.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.SObjectField;
import org.apache.camel.component.salesforce.api.utils.JsonUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SalesforceToRecordSchemaConverter {
    private static final ObjectMapper OBJECT_MAPPER = JsonUtils.createObjectMapper();
    private final String dateFormat;
    private final String dateTimeFormat;
    private final String timeFormat;

    public SalesforceToRecordSchemaConverter(String dateFormat, String dateTimeFormat, String timeFormat) {
        this.dateFormat = dateFormat;
        this.dateTimeFormat = dateTimeFormat;
        this.timeFormat = timeFormat;
    }

    public SObjectDescription getSalesforceObject(InputStream salesforceObjectResultJsonString) throws IOException {
        return OBJECT_MAPPER.readValue(salesforceObjectResultJsonString, SObjectDescription.class);
    }

    public RecordSchema convertSchema(SObjectDescription salesforceObject, String fieldNamesOfInterest) {
        List<SObjectField> fields = salesforceObject.getFields();
        if (StringUtils.isNotBlank(fieldNamesOfInterest)) {
            final List<String> listOfFieldNamesOfInterest = Arrays.asList(fieldNamesOfInterest.toLowerCase().split("\\s*,\\s*"));
            fields = fields
                    .stream()
                    .filter(sObjectField -> listOfFieldNamesOfInterest.contains(sObjectField.getName().toLowerCase()))
                    .collect(Collectors.toList());
        }

        final List<RecordField> recordFields = new ArrayList<>();

        for (SObjectField field : fields) {
            final String soapType = field.getSoapType();

            switch (soapType.substring(soapType.indexOf(':') + 1)) {
                case "ID":
                case "string":
                case "json":
                case "base64Binary":
                case "anyType":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.STRING.getDataType(), field.getDefaultValue(), field.isNillable()));
                    break;
                case "int":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.INT.getDataType(), field.getDefaultValue(), field.isNillable()));
                    break;
                case "long":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.LONG.getDataType(), field.getDefaultValue(), field.isNillable()));
                    break;
                case "double":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.DOUBLE.getDataType(), field.getDefaultValue(), field.isNillable()));
                    break;
                case "boolean":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.BOOLEAN.getDataType(), field.getDefaultValue(), field.isNillable()));
                    break;
                case "date":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.DATE.getDataType(dateFormat), field.getDefaultValue(), field.isNillable()));
                    break;
                case "dateTime":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.TIMESTAMP.getDataType(dateTimeFormat), field.getDefaultValue(), field.isNillable()));
                    break;
                case "time":
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.TIME.getDataType(timeFormat), field.getDefaultValue(), field.isNillable()));
                    break;
                case "address":
                    final RecordSchema addressSchema = new SimpleRecordSchema(Arrays.asList(
                            new RecordField("city", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("country", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("countryCode", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("postalCode", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("state", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("stateCode", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("street", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("geocodeAccuracy", RecordFieldType.STRING.getDataType(), true)
                    ));
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.RECORD.getRecordDataType(addressSchema), field.getDefaultValue(), field.isNillable()));
                    break;
                case "location":
                    final RecordSchema locationSchema = new SimpleRecordSchema(Arrays.asList(
                            new RecordField("latitude", RecordFieldType.STRING.getDataType(), true),
                            new RecordField("longitude", RecordFieldType.STRING.getDataType(), true)
                    ));
                    recordFields.add(new RecordField(field.getName(), RecordFieldType.RECORD.getRecordDataType(locationSchema), field.getDefaultValue(), field.isNillable()));
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Could not determine schema for '%s'. Could not convert field '%s' of soap type '%s'.",
                            salesforceObject.getName(), field.getName(), soapType));
            }
        }

        return new SimpleRecordSchema(recordFields);
    }
}

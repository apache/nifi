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
package org.apache.nifi.processors.salesforce.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.SObjectField;
import org.apache.camel.component.salesforce.api.utils.JsonUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
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
            fields = filterFieldsOfInterest(fields, fieldNamesOfInterest);
        }
        List<RecordField> recordFields = null;
        try {
            recordFields = convertSObjectFieldsToRecordFields(fields);
        } catch (IllegalArgumentException e) {
            throw new ProcessException(String.format("Could not determine schema for '%s'", salesforceObject.getName()), e);
        }

        return new SimpleRecordSchema(recordFields);
    }

    private List<RecordField> convertSObjectFieldsToRecordFields(List<SObjectField> fields) {
        List<RecordField> recordFields = new ArrayList<>();
        for (SObjectField field : fields) {
            recordFields.add(convertSObjectFieldToRecordField(field));
        }
        return recordFields;
    }

    private List<SObjectField> filterFieldsOfInterest(List<SObjectField> fields, String fieldNamesOfInterest) {
        List<String> listOfFieldNamesOfInterest = Arrays.asList(fieldNamesOfInterest.toLowerCase().split("\\s*,\\s*"));
        return fields
                .stream()
                .filter(sObjectField -> listOfFieldNamesOfInterest.contains(sObjectField.getName().toLowerCase()))
                .collect(Collectors.toList());
    }

    private RecordField convertSObjectFieldToRecordField(SObjectField field) {
        String soapType = field.getSoapType();
        DataType dataType = switch (soapType.substring(soapType.indexOf(':') + 1)) {
            case "ID", "string", "json", "base64Binary", "anyType" -> RecordFieldType.STRING.getDataType();
            case "int" -> RecordFieldType.INT.getDataType();
            case "long" -> RecordFieldType.LONG.getDataType();
            case "double" -> RecordFieldType.DOUBLE.getDataType();
            case "boolean" -> RecordFieldType.BOOLEAN.getDataType();
            case "date" -> RecordFieldType.DATE.getDataType(dateFormat);
            case "dateTime" -> RecordFieldType.TIMESTAMP.getDataType(dateTimeFormat);
            case "time" -> RecordFieldType.TIME.getDataType(timeFormat);
            case "address" -> RecordFieldType.RECORD.getRecordDataType(createAddressSchema());
            case "location" -> RecordFieldType.RECORD.getRecordDataType(createLocationSchema());
            default ->
                    throw new IllegalArgumentException(String.format("Could not convert field '%s' of soap type '%s'.", field.getName(), soapType));
        };
        return new RecordField(field.getName(), dataType, field.getDefaultValue(), field.isNillable());
    }

    private RecordSchema createAddressSchema() {
        return new SimpleRecordSchema(Arrays.asList(
                new RecordField("city", RecordFieldType.STRING.getDataType(), true),
                new RecordField("country", RecordFieldType.STRING.getDataType(), true),
                new RecordField("countryCode", RecordFieldType.STRING.getDataType(), true),
                new RecordField("postalCode", RecordFieldType.STRING.getDataType(), true),
                new RecordField("state", RecordFieldType.STRING.getDataType(), true),
                new RecordField("stateCode", RecordFieldType.STRING.getDataType(), true),
                new RecordField("street", RecordFieldType.STRING.getDataType(), true),
                new RecordField("geocodeAccuracy", RecordFieldType.STRING.getDataType(), true)
        ));
    }

    private RecordSchema createLocationSchema() {
        return new SimpleRecordSchema(Arrays.asList(
                new RecordField("latitude", RecordFieldType.STRING.getDataType(), true),
                new RecordField("longitude", RecordFieldType.STRING.getDataType(), true)
        ));
    }
}

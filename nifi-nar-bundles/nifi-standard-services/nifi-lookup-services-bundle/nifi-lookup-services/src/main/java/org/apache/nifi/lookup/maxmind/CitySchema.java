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

package org.apache.nifi.lookup.maxmind;

import java.util.Arrays;
import java.util.List;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class CitySchema {
    static final RecordField SUBDIVISION_NAME = new RecordField("name", RecordFieldType.STRING.getDataType());
    static final RecordField SUBDIVISION_ISO = new RecordField("isoCode", RecordFieldType.STRING.getDataType());

    private static final List<RecordField> SUBDIVISION_FIELDS = Arrays.asList(SUBDIVISION_NAME, SUBDIVISION_ISO);
    static final RecordSchema SUBDIVISION_SCHEMA = new SimpleRecordSchema(SUBDIVISION_FIELDS);
    static final DataType SUBDIVISION_DATA_TYPE = RecordFieldType.RECORD.getRecordDataType(SUBDIVISION_SCHEMA);

    static final RecordField COUNTRY_NAME = new RecordField("name", RecordFieldType.STRING.getDataType());
    static final RecordField COUNTRY_ISO = new RecordField("isoCode", RecordFieldType.STRING.getDataType());
    private static final List<RecordField> COUNTRY_FIELDS = Arrays.asList(COUNTRY_NAME, COUNTRY_ISO);
    static final RecordSchema COUNTRY_SCHEMA = new SimpleRecordSchema(COUNTRY_FIELDS);

    static final RecordField CITY = new RecordField("city", RecordFieldType.STRING.getDataType());
    static final RecordField ACCURACY = new RecordField("accuracy", RecordFieldType.INT.getDataType());
    static final RecordField METRO_CODE = new RecordField("metroCode", RecordFieldType.INT.getDataType());
    static final RecordField TIMEZONE = new RecordField("timeZone", RecordFieldType.STRING.getDataType());
    static final RecordField LATITUDE = new RecordField("latitude", RecordFieldType.DOUBLE.getDataType());
    static final RecordField LONGITUDE = new RecordField("longitude", RecordFieldType.DOUBLE.getDataType());
    static final RecordField SUBDIVISIONS = new RecordField("subdivisions", RecordFieldType.ARRAY.getArrayDataType(SUBDIVISION_DATA_TYPE));
    static final RecordField COUNTRY = new RecordField("country", RecordFieldType.RECORD.getRecordDataType(COUNTRY_SCHEMA));
    static final RecordField CONTINENT = new RecordField("continent", RecordFieldType.STRING.getDataType());
    static final RecordField POSTALCODE = new RecordField("postalCode", RecordFieldType.STRING.getDataType());

    private static final List<RecordField> GEO_FIELDS = Arrays.asList(CITY, ACCURACY, LATITUDE, LONGITUDE, SUBDIVISIONS, COUNTRY, POSTALCODE);
    static final RecordSchema GEO_SCHEMA = new SimpleRecordSchema(GEO_FIELDS);
}

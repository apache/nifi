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

package org.apache.nifi.serialization;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.serialization.record.RecordFieldType;

public class DateTimeUtils {
    public static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor.Builder()
        .name("Date Format")
        .description("Specifies the format to use when reading/writing Date fields")
        .expressionLanguageSupported(false)
        .defaultValue(RecordFieldType.DATE.getDefaultFormat())
        .addValidator(new SimpleDateFormatValidator())
        .required(true)
        .build();

    public static final PropertyDescriptor TIME_FORMAT = new PropertyDescriptor.Builder()
        .name("Time Format")
        .description("Specifies the format to use when reading/writing Time fields")
        .expressionLanguageSupported(false)
        .defaultValue(RecordFieldType.TIME.getDefaultFormat())
        .addValidator(new SimpleDateFormatValidator())
        .required(true)
        .build();

    public static final PropertyDescriptor TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
        .name("Timestamp Format")
        .description("Specifies the format to use when reading/writing Timestamp fields")
        .expressionLanguageSupported(false)
        .defaultValue(RecordFieldType.TIMESTAMP.getDefaultFormat())
        .addValidator(new SimpleDateFormatValidator())
        .required(true)
        .build();
}

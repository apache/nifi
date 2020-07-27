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

public class DateTimeUtils {
    public static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor.Builder()
        .name("Date Format")
        .description("Specifies the format to use when reading/writing Date fields. "
            + "If not specified, Date fields will be assumed to be number of milliseconds since epoch (Midnight, Jan 1, 1970 GMT). "
            + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy for a two-digit month, followed by "
            + "a two-digit day, followed by a four-digit year, all separated by '/' characters, as in 01/01/2017).")
        .expressionLanguageSupported(false)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .build();

    public static final PropertyDescriptor TIME_FORMAT = new PropertyDescriptor.Builder()
        .name("Time Format")
        .description("Specifies the format to use when reading/writing Time fields. "
            + "If not specified, Time fields will be assumed to be number of milliseconds since epoch (Midnight, Jan 1, 1970 GMT). "
            + "If specified, the value must match the Java Simple Date Format (for example, HH:mm:ss for a two-digit hour in 24-hour format, followed by "
            + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 18:04:15).")
        .expressionLanguageSupported(false)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .build();

    public static final PropertyDescriptor TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
        .name("Timestamp Format")
        .description("Specifies the format to use when reading/writing Timestamp fields. "
            + "If not specified, Timestamp fields will be assumed to be number of milliseconds since epoch (Midnight, Jan 1, 1970 GMT). "
            + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy HH:mm:ss for a two-digit month, followed by "
            + "a two-digit day, followed by a four-digit year, all separated by '/' characters; and then followed by a two-digit hour in 24-hour format, followed by "
            + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 01/01/2017 18:04:15).")
        .expressionLanguageSupported(false)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .build();
}

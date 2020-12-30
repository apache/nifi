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
package org.apache.nifi.util.db;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public class JdbcProperties {

    public static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
            .name("dbf-normalize")
            .displayName("Normalize Table/Column Names")
            .description("Whether to change non-Avro-compatible characters in column names to Avro-compatible characters. For example, colons and periods "
                    + "will be changed to underscores in order to build a valid Avro record.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor USE_AVRO_LOGICAL_TYPES = new PropertyDescriptor.Builder()
            .name("dbf-user-logical-types")
            .displayName("Use Avro Logical Types")
            .description("Whether to use Avro Logical Types for DECIMAL/NUMBER, DATE, TIME and TIMESTAMP columns. "
                    + "If disabled, written as string. "
                    + "If enabled, Logical types are used and written as its underlying type, specifically, "
                    + "DECIMAL/NUMBER as logical 'decimal': written as bytes with additional precision and scale meta data, "
                    + "DATE as logical 'date-millis': written as int denoting days since Unix epoch (1970-01-01), "
                    + "TIME as logical 'time-millis': written as int denoting milliseconds since Unix epoch, "
                    + "and TIMESTAMP as logical 'timestamp-millis': written as long denoting milliseconds since Unix epoch. "
                    + "If a reader of written Avro records also knows these logical types, then these values can be deserialized with more context depending on reader implementation.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_PRECISION = new PropertyDescriptor.Builder()
            .name("dbf-default-precision")
            .displayName("Default Decimal Precision")
            .description("When a DECIMAL/NUMBER value is written as a 'decimal' Avro logical type,"
                    + " a specific 'precision' denoting number of available digits is required."
                    + " Generally, precision is defined by column data type definition or database engines default."
                    + " However undefined precision (0) can be returned from some database engines."
                    + " 'Default Decimal Precision' is used when writing those undefined precision numbers.")
            .defaultValue(String.valueOf(JdbcCommon.DEFAULT_PRECISION_VALUE))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_SCALE = new PropertyDescriptor.Builder()
            .name("dbf-default-scale")
            .displayName("Default Decimal Scale")
            .description("When a DECIMAL/NUMBER value is written as a 'decimal' Avro logical type,"
                    + " a specific 'scale' denoting number of available decimal digits is required."
                    + " Generally, scale is defined by column data type definition or database engines default."
                    + " However when undefined precision (0) is returned, scale can also be uncertain with some database engines."
                    + " 'Default Decimal Scale' is used when writing those undefined numbers."
                    + " If a value has more decimals than specified scale, then the value will be rounded-up,"
                    + " e.g. 1.53 becomes 2 with scale 0, and 1.5 with scale 1.")
            .defaultValue(String.valueOf(JdbcCommon.DEFAULT_SCALE_VALUE))
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    // Registry-only versions of Default Precision and Default Scale properties
    public static final PropertyDescriptor VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(DEFAULT_PRECISION)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();

    public static final PropertyDescriptor VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(DEFAULT_SCALE)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();
}

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

package org.apache.nifi.csv;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RowRecordReaderFactory;
import org.apache.nifi.serialization.UserTypeOverrideRowReader;

@Tags({"csv", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("Parses CSV-formatted data, returning each row in the CSV file as a separate record. "
    + "This reader assumes that the first line in the content is the column names and all subsequent lines are "
    + "the values. By default, the reader will assume that all columns are of 'String' type, but this can be "
    + "overridden by adding a user-defined Property where the key is the name of a column and the value is the "
    + "type of the column. For example, if a Property has the name \"balance\" with a value of float, it the "
    + "reader will attempt to coerce all values in the \"balance\" column into a floating-point number. See "
    + "Controller Service's Usage for further documentation.")
@DynamicProperty(name = "<name of column in CSV>", value = "<type of column values in CSV>",
    description = "User-defined properties are used to indicate that the values of a specific column should be interpreted as a "
    + "user-defined data type (e.g., int, double, float, date, etc.)", supportsExpressionLanguage = false)
public class CSVReader extends UserTypeOverrideRowReader implements RowRecordReaderFactory {

    @Override
    public RecordReader createRecordReader(final InputStream in, final ComponentLog logger) throws IOException {
        return new CSVRecordReader(in, logger, getFieldTypeOverrides());
    }

}

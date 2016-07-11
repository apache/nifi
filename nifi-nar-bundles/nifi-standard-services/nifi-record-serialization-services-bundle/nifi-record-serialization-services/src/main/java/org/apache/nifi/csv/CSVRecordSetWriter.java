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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;

@Tags({"csv", "result", "set", "writer", "serializer", "record", "row"})
@CapabilityDescription("Writes the contents of a Database ResultSet as CSV data. The first line written "
    + "will be the column names. All subsequent lines will be the values corresponding to those columns.")
public class CSVRecordSetWriter extends AbstractRecordSetWriter implements RecordSetWriterFactory {

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger) {
        return new WriteCSVResult(getDateFormat(), getTimeFormat(), getTimestampFormat());
    }

}

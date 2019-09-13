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
package org.apache.nifi.text;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class TextLineRecordReader implements RecordReader {

    private final ComponentLog log;
    private final RecordSchema schema;
    private final String fieldName;
    private final Scanner lineReader;
    private final String lineDelimiter;
    private int linesLeftToSkip;
    private final int linesPerGroup;
    private final boolean ignoreEmptyLines;

    public TextLineRecordReader(final InputStream in, final ComponentLog logger, final String fieldName,
                                int skipLineCount, int linesPerGroup, boolean ignoreEmptyLines, String lineDelimiter, String charSet) {
        this.log = logger;
        this.fieldName = fieldName;
        this.lineReader = new Scanner(in, charSet).useDelimiter("(?<=" + lineDelimiter + ")");
        this.lineDelimiter = lineDelimiter;
        this.linesLeftToSkip = skipLineCount;
        this.linesPerGroup = linesPerGroup;
        this.ignoreEmptyLines = ignoreEmptyLines;
        final List<RecordField> fieldList = Collections.singletonList(new RecordField(fieldName, RecordFieldType.STRING.getDataType()));
        schema = new SimpleRecordSchema(fieldList);
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (!lineReader.hasNext() && !lineReader.hasNextLine()) {
            return null;
        }
        while (linesLeftToSkip > 0) {
            linesLeftToSkip--;
            if (!lineReader.hasNext()) {
                continue;
            }
            lineReader.next();
        }
        // Read the lines
        StringBuilder stringBuilder = new StringBuilder();
        boolean linesFound = false;
        for (int i = 0; i < linesPerGroup; i++) {
            String line;
            if (!lineReader.hasNext()) {
                // This might be the last line in the file and may not have a trailing delimiter, so try nextLine()
                if (lineReader.hasNextLine()) {
                    line = lineReader.nextLine();
                } else {
                    break;
                }
            } else {
                line = lineReader.next();
            }
            // next() may return the token with the delimiter, so remove it if present
            if (line.endsWith(lineDelimiter)) {
                line = line.substring(0, line.length() - lineDelimiter.length());
            }

            if (ignoreEmptyLines && line.isEmpty()) {
                // Pretend this line never happened, reset the loop variable and keep going
                i--;
                continue;
            }
            if (linesPerGroup > 1 && i > 0) {
                stringBuilder.append(lineDelimiter);
            }

            stringBuilder.append(line);

            // We've done an append by now, so set the linesFound flag. This way we can tell the difference between the original (untouched) StringBuilder
            // and one where we've appended empty strings.
            linesFound = true;
        }

        // One more check for a fully-empty value, should only happen with 1 line per group when that line is empty. Otherwise the record should be built and returned
        String fieldValue = stringBuilder.toString();
        if (!linesFound || (ignoreEmptyLines && fieldValue.isEmpty())) {
            return null;
        } else {
            return new MapRecord(schema, Collections.singletonMap(fieldName, fieldValue));
        }
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return schema;
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}

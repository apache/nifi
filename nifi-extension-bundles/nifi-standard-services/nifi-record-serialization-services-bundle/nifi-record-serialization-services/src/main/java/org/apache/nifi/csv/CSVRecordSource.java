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
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.inference.RecordSource;

public class CSVRecordSource implements RecordSource<CSVRecordAndFieldNames> {
    private final Iterator<CSVRecord> csvRecordIterator;
    private final List<String> fieldNames;

    public CSVRecordSource(final InputStream in, final PropertyContext context, final Map<String, String> variables) throws IOException {
        final String charset = context.getProperty(CSVUtils.CHARSET).getValue();

        final Reader reader;
        try {
            reader = new InputStreamReader(BOMInputStream.builder().setInputStream(in).get(), charset);
        } catch (UnsupportedEncodingException e) {
            throw new ProcessException(e);
        }

        final CSVFormat csvFormat = CSVUtils.createCSVFormat(context, variables).builder().setHeader().setSkipHeaderRecord(true).setTrim(true).get();
        final CSVParser csvParser = CSVParser.builder()
                .setReader(reader)
                .setFormat(csvFormat)
                .get();
        fieldNames = List.copyOf(csvParser.getHeaderMap().keySet());

        csvRecordIterator = csvParser.iterator();
    }

    @Override
    public CSVRecordAndFieldNames next() {
        if (csvRecordIterator.hasNext()) {
            final CSVRecord record = csvRecordIterator.next();
            return new CSVRecordAndFieldNames(record, fieldNames);
        }

        return null;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}

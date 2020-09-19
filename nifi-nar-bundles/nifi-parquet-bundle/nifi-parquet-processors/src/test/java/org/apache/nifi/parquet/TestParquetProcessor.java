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
package org.apache.nifi.parquet;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestParquetProcessor extends AbstractProcessor {

    public static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
            .name("reader")
            .description("reader")
            .identifiesControllerService(ParquetReader.class)
            .required(true)
            .build();

    public static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("path")
            .description("path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("success")
            .build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        final RecordReaderFactory readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);

        final List<String> records = new ArrayList<>();

        byte[] parquetBytes;

        // read the parquet file into bytes since we can't use a FileInputStream since it doesn't support mark/reset
        try {
            parquetBytes = IOUtils.toByteArray(new File(context.getProperty(PATH).getValue()).toURI());
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        try (final InputStream in = new ByteArrayInputStream(parquetBytes);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
                records.add(record.toString());
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        flowFile = session.write(flowFile, (out) -> out.write(StringUtils.join(records, "\n").getBytes()));
        session.transfer(flowFile, SUCCESS);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<PropertyDescriptor>() {{ add(READER); add(PATH); }};
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<Relationship>() {{ add(SUCCESS); }};
    }

}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;
;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TestInferAvroSchemaFromCSV {

    private final String CSV_HEADER_LINE = "fname,lname,age,zip";

    @Test
    public void inferSchemaFromHeaderLineOfCSV() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(InferAvroSchemaFromCSV.class);

        runner.assertNotValid();
        runner.setProperty(InferAvroSchemaFromCSV.HEADER_LINE_SKIP_COUNT, "0");
        runner.setProperty(InferAvroSchemaFromCSV.ESCAPE_STRING, "\\");
        runner.setProperty(InferAvroSchemaFromCSV.QUOTE_STRING, "'");
        runner.setProperty(InferAvroSchemaFromCSV.RECORD_NAME, "contact");
        runner.setProperty(InferAvroSchemaFromCSV.CHARSET, "UTF-8");
        runner.setProperty(InferAvroSchemaFromCSV.PRETTY_AVRO_OUTPUT, "true");

        runner.assertValid();

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.write(session.create(), new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write((CSV_HEADER_LINE + "\nJeremy,Dyer,29,55555").getBytes());
            }
        });

        //Enqueue the empty FlowFile
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_SUCCESS, 1);
    }

    @Test
    public void inferSchemaFormHeaderLinePropertyOfProcessor() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(InferAvroSchemaFromCSV.class);

        runner.assertNotValid();
        runner.setProperty(InferAvroSchemaFromCSV.HEADER_LINE, CSV_HEADER_LINE);
        runner.setProperty(InferAvroSchemaFromCSV.HEADER_LINE_SKIP_COUNT, "1");
        runner.setProperty(InferAvroSchemaFromCSV.ESCAPE_STRING, "\\");
        runner.setProperty(InferAvroSchemaFromCSV.QUOTE_STRING, "'");
        runner.setProperty(InferAvroSchemaFromCSV.RECORD_NAME, "contact");
        runner.setProperty(InferAvroSchemaFromCSV.CHARSET, "UTF-8");
        runner.setProperty(InferAvroSchemaFromCSV.PRETTY_AVRO_OUTPUT, "true");

        runner.assertValid();

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.write(session.create(), new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                out.write((CSV_HEADER_LINE + "\nJeremy,Dyer,29,55555").getBytes());
            }
        });

        //Enqueue the empty FlowFile
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_SUCCESS, 1);
    }

    @Test
    public void inferSchemaFromEmptyContent() throws Exception  {
        TestRunner runner = TestRunners.newTestRunner(InferAvroSchemaFromCSV.class);

        runner.assertNotValid();
        runner.setProperty(InferAvroSchemaFromCSV.HEADER_LINE, CSV_HEADER_LINE);
        runner.setProperty(InferAvroSchemaFromCSV.HEADER_LINE_SKIP_COUNT, "1");
        runner.setProperty(InferAvroSchemaFromCSV.ESCAPE_STRING, "\\");
        runner.setProperty(InferAvroSchemaFromCSV.QUOTE_STRING, "'");
        runner.setProperty(InferAvroSchemaFromCSV.RECORD_NAME, "contact");
        runner.setProperty(InferAvroSchemaFromCSV.CHARSET, "UTF-8");
        runner.setProperty(InferAvroSchemaFromCSV.PRETTY_AVRO_OUTPUT, "true");

        runner.assertValid();

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.write(session.create(), new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                out.write("".getBytes());
            }
        });

        //Enqueue the empty FlowFile
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_FAILURE, 1);
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_ORIGINAL, 0);
        runner.assertTransferCount(InferAvroSchemaFromCSV.REL_SUCCESS, 0);
    }

}

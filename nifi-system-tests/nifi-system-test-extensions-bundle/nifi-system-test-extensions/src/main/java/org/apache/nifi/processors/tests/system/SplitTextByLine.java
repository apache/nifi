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
package org.apache.nifi.processors.tests.system;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SplitTextByLine extends AbstractProcessor {

    static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .build();
    static final Relationship SPLITS = new Relationship.Builder()
            .name("splits")
            .build();
    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(ORIGINAL, SPLITS, FAILURE));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final List<String> splits = new ArrayList<>();

        try (final InputStream in = session.read(flowFile);
             final Reader streamReader = new InputStreamReader(in);
             final BufferedReader reader = new BufferedReader(streamReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                splits.add(line);
            }
        } catch (IOException ioe) {
            session.transfer(flowFile, FAILURE);
        }

        long offset = 0;
        for (String splitText : splits) {
            FlowFile splitFlowFile = session.clone(flowFile, offset, splitText.getBytes(StandardCharsets.UTF_8).length);
            offset = offset + splitText.getBytes(StandardCharsets.UTF_8).length;
            splitFlowFile = session.write(splitFlowFile, out -> out.write(splitText.getBytes(StandardCharsets.UTF_8)));
            session.transfer(splitFlowFile, SPLITS);
        }
        session.transfer(flowFile, ORIGINAL);
    }
}

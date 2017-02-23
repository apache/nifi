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
package org.apache.nifi.schemaregistry.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.StreamCallback;

/**
 * Base processor which contains common functionality for processors that
 * receive {@link FlowFile} and output {@link FlowFile} while also modifying the
 * content of the {@link FlowFile}
 */
public abstract class BaseContentTransformer extends BaseTransformer {

    @Override
    protected FlowFile doTransform(ProcessContext context, ProcessSession session, FlowFile flowFile, InvocationContextProperties contextProperties) {
        AtomicReference<Map<String, String>> attributeRef = new AtomicReference<Map<String, String>>();
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                attributeRef.set(transform(in, out, contextProperties));
            }
        });
        if (attributeRef.get() != null) {
            flowFile = session.putAllAttributes(flowFile, attributeRef.get());
        }
        return flowFile;
    }
}

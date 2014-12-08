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
package org.apache.nifi.scripting;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptException;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.io.BufferedInputStream;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

/**
 * <p>
 * Script authors should extend this class if they want to perform complex
 * conversions in a NiFi processor.
 * </p>
 *
 * <p>
 * Scripts must implement {@link #convert(FileInputStream)}. This method may
 * create new FlowFiles and pass them to one or more routes. The input FlowFile
 * will be removed from the repository after execution of this method completes.
 * </p>
 *
 * <p>
 * In general, the {@link #convert(FileInputStream)} will read from the supplied
 * stream, then create one or more output sinks and route the result to the
 * relationship of choice using
 * {@link #routeStream(ByteArrayOutputStream, String, String)} or
 * {@link #routeBytes(byte[], String, String)}.
 *
 * <p>
 * Implement {@link #getProcessorRelationships()} to allow writing to
 * relationships other than <code>success</code> and <code>failure</code>. The
 * {@link #getRoute()} superclass method is *not* used by Converter Scripts.
 * </p>
 *
 */
public class ConverterScript extends Script {

    private ProcessSession session; // used to create files
    private Object convertCallback;

    public ConverterScript() {

    }

    public ConverterScript(Object... callbacks) {
        super(callbacks);
        for (Object callback : callbacks) {
            if (callback instanceof Map<?, ?>) {
                convertCallback = convertCallback == null && ((Map<?, ?>) callback).containsKey("convert") ? callback : convertCallback;
            }
        }
    }

    // Subclasses should implement this to define basic logic
    protected void convert(InputStream stream) throws NoSuchMethodException, ScriptException {
        if (convertCallback != null) {
            ((Invocable) engine).invokeMethod(convertCallback, "convert", stream);
        }
    }

    /**
     * Owning processor uses this method to kick off handling of a single file
     *
     * @param aSession the owning processor's Repository (needed to make new
     * files)
     */
    public void process(ProcessSession aSession) {
        this.session = aSession;
        this.session.read(this.flowFile, new InputStreamCallback() {

            @Override
            public void process(InputStream in) throws IOException {
                BufferedInputStream stream = new BufferedInputStream(in);
                try {
                    convert(stream);
                } catch (NoSuchMethodException | ScriptException e) {
                    logger.error("Failed to execute 'convert' function in script", e);
                    throw new IOException(e);
                }
            }
        });
    }

    // this should go back to protected once we get Nashorn
    public void createFlowFile(final String flowFileName, final Relationship relationship, final OutputStreamHandler handler) {
        FlowFile result = session.create(this.flowFile);
        result = session.putAttribute(result, CoreAttributes.FILENAME.key(), flowFileName);
        try {
            result = session.write(result, new OutputStreamCallback() {

                @Override
                public void process(OutputStream out) throws IOException {
                    handler.write(out);
                }
            });
            this.logger.info("Transfer flow file {} to {}", new Object[]{result, relationship});
            session.transfer(result, relationship);
        } catch (Exception e) {
            this.logger.error("Could not create new flow file from script", e);
            session.remove(result);
        }
    }

}

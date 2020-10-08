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
package org.apache.nifi.controller.repository.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.FlowFileAccessException;

/**
 * <p>
 * Wraps an OutputStream so that if any IOException is thrown, it will be wrapped in a FlowFileAccessException. We do this to isolate IOExceptions thrown by the framework from those thrown by user
 * code. If thrown by the framework, it generally indicates a problem communicating with the Content Repository (such as out of disk space) and session rollback is often appropriate so that the
 * FlowFile can be processed again.
 * </p>
 */
public class FlowFileAccessOutputStream extends FilterOutputStream {

    private final FlowFile flowFile;
    private final OutputStream out;

    public FlowFileAccessOutputStream(final OutputStream out, final FlowFile flowFile) {
        super(out);
        this.flowFile = flowFile;
        this.out = out;
    }

    @Override
    public void close() throws IOException {
        try {
            out.flush();
        } catch (final IOException ioe) {
        }

        try {
            out.close();
        } catch (final IOException ioe) {
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        try {
            out.write(b, off, len);
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not write to " + flowFile, ioe);
        }
    }

    @Override
    public void write(final int b) throws IOException {
        try {
            out.write(b);
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not write to " + flowFile, ioe);
        }
    }

    @Override
    public void write(final byte[] b) throws IOException {
        try {
            out.write(b);
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not write to " + flowFile, ioe);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            out.flush();
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not flush OutputStream for " + flowFile, ioe);
        }
    }
}

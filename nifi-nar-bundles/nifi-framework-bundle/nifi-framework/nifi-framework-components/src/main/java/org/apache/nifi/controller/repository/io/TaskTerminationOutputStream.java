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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.processor.exception.TerminatedTaskException;

public class TaskTerminationOutputStream extends OutputStream {
    private final TaskTermination taskTermination;
    private final OutputStream delegate;
    private final Runnable terminatedCallback;

    public TaskTerminationOutputStream(final OutputStream delegate, final TaskTermination taskTermination, final Runnable terminatedCallback) {
        this.delegate = delegate;
        this.taskTermination = taskTermination;
        this.terminatedCallback = terminatedCallback;
    }

    private void verifyNotTerminated() {
        if (taskTermination.isTerminated()) {
            final TerminatedTaskException tte = new TerminatedTaskException();

            if (terminatedCallback != null) {
                try {
                    terminatedCallback.run();
                } catch (final Exception e) {
                    tte.addSuppressed(e);
                }
            }

            throw tte;
        }
    }

    @Override
    public void write(final int b) throws IOException {
        verifyNotTerminated();
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        verifyNotTerminated();
        delegate.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        verifyNotTerminated();
        delegate.write(b);
    }

    @Override
    public void flush() throws IOException {
        verifyNotTerminated();
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } catch (final Exception e) {
            if (taskTermination.isTerminated()) {
                final TerminatedTaskException tte = new TerminatedTaskException();
                tte.addSuppressed(e);

                if (terminatedCallback != null) {
                    try {
                        terminatedCallback.run();
                    } catch (final Exception callbackException) {
                        tte.addSuppressed(callbackException);
                    }
                }

                throw tte;
            }
        }

        verifyNotTerminated();
    }
}

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
package org.apache.nifi.processors.standard;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"command", "process", "source", "external", "invoke", "script"})
@CapabilityDescription("Runs an operating system command specified by the user and writes the output of that command to a FlowFile. If the command is expected "
        + "to be long-running, the Processor can output the partial data on a specified interval. When this option is used, the output is expected to be in textual "
        + "format, as it typically does not make sense to split binary data on arbitrary time-based intervals.")
@DynamicProperty(name="An environment variable name", description="An environment variable value")
public class ExecuteProcess extends AbstractProcessor {

    public static final PropertyDescriptor COMMAND = new PropertyDescriptor.Builder()
            .name("Command")
            .description("Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COMMAND_ARGUMENTS = new PropertyDescriptor.Builder()
            .name("Command Arguments")
            .description("The arguments to supply to the executable delimited by white space. White space can be escaped by enclosing it in double-quotes.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
            .name("Working Directory")
            .description("The directory to use as the current working directory when executing the command")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, true))
            .required(false)
            .build();

    public static final PropertyDescriptor BATCH_DURATION = new PropertyDescriptor.Builder()
            .name("Batch Duration")
            .description("If the process is expected to be long-running and produce textual output, a batch duration can be specified so "
                    + "that the output will be captured for this amount of time and a FlowFile will then be sent out with the results "
                    + "and a new FlowFile will be started, rather than waiting for the process to finish before sending out the results")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIRECT_ERROR_STREAM = new PropertyDescriptor.Builder()
            .name("Redirect Error Stream")
            .description("If true will redirect any error stream output of the process to the output stream. "
                    + "This is particularly helpful for processes which write extensively to the error stream or for troubleshooting.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All created FlowFiles are routed to this relationship")
            .build();

    private volatile ExecutorService executor;

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(COMMAND);
        properties.add(COMMAND_ARGUMENTS);
        properties.add(BATCH_DURATION);
        properties.add(REDIRECT_ERROR_STREAM);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Sets the environment variable '" + propertyDescriptorName + "' for the process' environment")
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    static List<String> splitArgs(final String input) {
        if (input == null) {
            return Collections.emptyList();
        }

        final List<String> args = new ArrayList<>();

        final String trimmed = input.trim();
        boolean inQuotes = false;

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < trimmed.length(); i++) {
            final char c = trimmed.charAt(i);
            switch (c) {
                case ' ':
                case '\t':
                case '\r':
                case '\n': {
                    if (inQuotes) {
                        sb.append(c);
                    } else {
                        final String arg = sb.toString().trim();
                        if (!arg.isEmpty()) {
                            args.add(arg);
                        }
                        sb.setLength(0);
                    }
                    break;
                }
                case '"':
                    inQuotes = !inQuotes;
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }

        final String finalArg = sb.toString().trim();
        if (!finalArg.isEmpty()) {
            args.add(finalArg);
        }

        return args;
    }

    @OnScheduled
    public void setupExecutor(final ProcessContext context) {
        executor = Executors.newFixedThreadPool(context.getMaxConcurrentTasks() * 2, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = defaultFactory.newThread(r);
                t.setName("ExecuteProcess " + getIdentifier() + " Task");
                return t;
            }
        });
    }

    @OnUnscheduled
    public void shutdownExecutor() {
        executor.shutdown();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String command = context.getProperty(COMMAND).getValue();
        final List<String> args = splitArgs(context.getProperty(COMMAND_ARGUMENTS).getValue());
        final Boolean redirectErrorStream = context.getProperty(REDIRECT_ERROR_STREAM).asBoolean();

        final List<String> commandStrings = new ArrayList<>(args.size() + 1);
        commandStrings.add(command);
        commandStrings.addAll(args);

        final String commandString = StringUtils.join(commandStrings, " ");

        final ProcessBuilder builder = new ProcessBuilder(commandStrings);
        final String workingDirName = context.getProperty(WORKING_DIR).getValue();
        if (workingDirName != null) {
            builder.directory(new File(workingDirName));
        }

        final Map<String, String> environment = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), entry.getValue());
            }
        }

        if (!environment.isEmpty()) {
            builder.environment().putAll(environment);
        }

        final long startNanos = System.nanoTime();
        final Process process;
        try {
            process = builder.redirectErrorStream(redirectErrorStream).start();
        } catch (final IOException ioe) {
            getLogger().error("Failed to create process due to {}", new Object[]{ioe});
            context.yield();
            return;
        }

        final Long batchNanos = context.getProperty(BATCH_DURATION).asTimePeriod(TimeUnit.NANOSECONDS);

        // Submit task to read error stream from process
        if (!redirectErrorStream) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                        while (reader.read() >= 0) {
                        }
                    } catch (final IOException ioe) {
                    }
                }
            });
        }

        // Submit task to read output of Process and write to FlowFile.
        final ProxyOutputStream proxyOut = new ProxyOutputStream(getLogger());
        final AtomicBoolean failure = new AtomicBoolean(false);
        final AtomicBoolean finishedCopying = new AtomicBoolean(false);
        final Future<?> future = executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws IOException {
                try {
                    if (batchNanos == null) {
                        // if we aren't batching, just copy the stream from the process to the flowfile.
                        try (final BufferedInputStream bufferedIn = new BufferedInputStream(process.getInputStream())) {
                            final byte[] buffer = new byte[4096];
                            int len;
                            while ((len = bufferedIn.read(buffer)) > 0) {
                                if (!isScheduled()) {
                                    return null;
                                }

                                proxyOut.write(buffer, 0, len);
                            }
                        }
                    } else {
                        // we are batching, which means that the output of the process is text. It doesn't make sense to grab
                        // arbitrary batches of bytes from some process and send it along as a piece of data, so we assume that
                        // setting a batch during means text.
                        // Also, we don't want that text to get split up in the middle of a line, so we use BufferedReader
                        // to read lines of text and write them as lines of text.
                        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                            String line;

                            while ((line = reader.readLine()) != null) {
                                if (!isScheduled()) {
                                    return null;
                                }

                                proxyOut.write((line + "\n").getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    }
                } catch (final IOException ioe) {
                    failure.set(true);
                    throw ioe;
                } finally {
                    finishedCopying.set(true);
                }

                return null;
            }
        });

        // continue to do this loop until both the process has finished and we have finished copying
        // the output from the process to the FlowFile. Unfortunately, even after calling Process.exitValue(), 
        // there can be data buffered on the InputStream; so we will wait until the stream is empty as well.
        int flowFileCount = 0;
        while (!finishedCopying.get() || isAlive(process)) {
            if (!isScheduled()) {
                getLogger().info("User stopped processor; will terminate process immediately");
                process.destroy();
                break;
            }

            // Create a FlowFile that we can write to and set the OutputStream for the FlowFile
            // as the delegate for the ProxyOuptutStream, then wait until the process finishes
            // or until the specified amount of time
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream flowFileOut) throws IOException {
                    try (final OutputStream out = new BufferedOutputStream(flowFileOut)) {
                        proxyOut.setDelegate(out);

                        if (batchNanos == null) {
                            // we are not creating batches; wait until process terminates.
                            Integer exitCode = null;
                            while (exitCode == null) {
                                try {
                                    exitCode = process.waitFor();
                                } catch (final InterruptedException ie) {
                                }
                            }
                        } else {
                            // wait the allotted amount of time.
                            try {
                                TimeUnit.NANOSECONDS.sleep(batchNanos);
                            } catch (final InterruptedException ie) {
                            }
                        }

                        proxyOut.setDelegate(null); // prevent from writing to this stream
                    }
                }
            });

            if (flowFile.getSize() == 0L) {
                // If no data was written to the file, remove it
                session.remove(flowFile);
            } else if (failure.get()) {
                // If there was a failure processing the output of the Process, remove the FlowFile
                session.remove(flowFile);
                getLogger().error("Failed to read data from Process, so will not generate FlowFile");
                break;
            } else {
                // All was good. Generate event and transfer FlowFile.
                session.getProvenanceReporter().create(flowFile, "Created from command: " + commandString);
                getLogger().info("Created {} and routed to success", new Object[]{flowFile});
                session.transfer(flowFile, REL_SUCCESS);
                flowFileCount++;
            }

            // Commit the session so that the FlowFile is transferred to the next processor
            session.commit();
        }

        final int exitCode;
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        try {
            exitCode = process.waitFor();
        } catch (final InterruptedException ie) {
            getLogger().warn("Process was interrupted before finishing");
            return;
        }

        try {
            future.get();
        } catch (final ExecutionException e) {
            getLogger().error("Failed to copy output from Process to FlowFile due to {}", new Object[]{e.getCause()});
        } catch (final InterruptedException ie) {
            getLogger().error("Interrupted while waiting to copy data form Process to FlowFile");
            return;
        }

        getLogger().info("Process finished with exit code {} after creating {} FlowFiles in {} millis", new Object[]{exitCode, flowFileCount, millis});
    }

    private boolean isAlive(final Process process) {
        // unfortunately, java provides no straight-forward way to test if a Process is alive.
        // In Java 8, Process.isAlive() is introduced, but NiFi needs to run against Java 7,
        // so we have this solution in the mean time.
        try {
            process.exitValue();
            return false;
        } catch (final IllegalThreadStateException itse) {
            return true;
        }
    }

    /**
     * Output stream that is used to wrap another output stream in a way that
     * the underlying output stream can be swapped out for a different one when
     * needed
     */
    private static class ProxyOutputStream extends OutputStream {

        private final ProcessorLog logger;

        private final Lock lock = new ReentrantLock();
        private OutputStream delegate;

        public ProxyOutputStream(final ProcessorLog logger) {
            this.logger = logger;
        }

        public void setDelegate(final OutputStream delegate) {
            lock.lock();
            try {
                logger.trace("Switching delegate from {} to {}", new Object[]{this.delegate, delegate});

                this.delegate = delegate;
            } finally {
                lock.unlock();
            }
        }

        private void sleep(final long millis) {
            try {
                Thread.sleep(millis);
            } catch (final InterruptedException ie) {
            }
        }

        @Override
        public void write(final int b) throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        logger.trace("Writing to {}", new Object[]{delegate});

                        delegate.write(b);
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        logger.trace("Writing to {}", new Object[]{delegate});

                        delegate.write(b, off, len);
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void write(final byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void flush() throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        delegate.flush();
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}

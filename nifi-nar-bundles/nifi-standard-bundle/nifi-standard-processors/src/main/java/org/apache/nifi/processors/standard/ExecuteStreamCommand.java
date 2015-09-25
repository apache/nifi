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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.ArgumentUtils;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

/**
 * <p>
 * This processor executes an external command on the contents of a flow file, and creates a new flow file with the results of the command.
 * </p>
 * <p>
 * <strong>Properties:</strong>
 * </p>
 * <ul>
 * <li><strong>Command Path</strong>
 * <ul>
 * <li>Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.</li>
 * <li>Default value: none</li>
 * <li>Supports expression language: true</li>
 * </ul>
 * </li>
 * <li>Command Arguments
 * <ul>
 * <li>The arguments to supply to the executable delimited by the ';' character. Each argument may be an Expression Language statement.</li>
 * <li>Default value: none</li>
 * <li>Supports expression language: true</li>
 * </ul>
 * </li>
 * <li>Working Directory
 * <ul>
 * <li>The directory to use as the current working directory when executing the command</li>
 * <li>Default value: none (which means whatever NiFi's current working directory is...probably the root of the NiFi installation directory.)</li>
 * <li>Supports expression language: true</li>
 * </ul>
 * </li>
 * <li>Ignore STDIN
 * <ul>
 * <li>Indicates whether or not the flowfile's contents should be streamed as part of STDIN</li>
 * <li>Default value: false (this means that the contents of a flowfile will be sent as STDIN to your command</li>
 * <li>Supports expression language: false</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * <strong>Relationships:</strong>
 * </p>
 * <ul>
 * <li>original
 * <ul>
 * <li>The destination path for the original incoming flow file</li>
 * </ul>
 * </li>
 * <li>output-stream
 * <ul>
 * <li>The destination path for the flow file created from the command's output</li>
 * </ul>
 * </li>
 * </ul>
 * <p>
 *
 */
@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"command execution", "command", "stream", "execute"})
@CapabilityDescription("Executes an external command on the contents of a flow file, and creates a new flow file with the results of the command.")
@DynamicProperty(name = "An environment variable name", value = "An environment variable value", description = "These environment variables are passed to the process spawned by this Processor")
@WritesAttributes({
    @WritesAttribute(attribute = "execution.command", description = "The name of the command executed to create the new FlowFile"),
    @WritesAttribute(attribute = "execution.command.args", description = "The semi-colon delimited list of arguments"),
    @WritesAttribute(attribute = "execution.status", description = "The exit status code returned from executing the command"),
    @WritesAttribute(attribute = "execution.error", description = "Any error messages returned from executing the command")})
public class ExecuteStreamCommand extends AbstractProcessor {

    public static final Relationship ORIGINAL_RELATIONSHIP = new Relationship.Builder()
            .name("original")
            .description("FlowFiles that were successfully processed")
            .build();
    public static final Relationship OUTPUT_STREAM_RELATIONSHIP = new Relationship.Builder()
            .name("output stream")
            .description("The destination path for the flow file created from the command's output")
            .build();
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(OUTPUT_STREAM_RELATIONSHIP);
        rels.add(ORIGINAL_RELATIONSHIP);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    private static final Validator ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR = StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true);
    static final PropertyDescriptor EXECUTION_COMMAND = new PropertyDescriptor.Builder()
            .name("Command Path")
            .description("Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.")
            .expressionLanguageSupported(true)
            .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor EXECUTION_ARGUMENTS = new PropertyDescriptor.Builder()
            .name("Command Arguments")
            .description("The arguments to supply to the executable delimited by the ';' character.")
            .expressionLanguageSupported(true).addValidator(new Validator() {

                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                    ValidationResult result = new ValidationResult.Builder()
                    .subject(subject).valid(true).input(input).build();
                    List<String> args = ArgumentUtils.splitArgs(input, context.getProperty(ARG_DELIMITER).getValue().charAt(0));
                    for (String arg : args) {
                        ValidationResult valResult = ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR.validate(subject, arg, context);
                        if (!valResult.isValid()) {
                            result = valResult;
                            break;
                        }
                    }
                    return result;
                }
            }).build();

    static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
            .name("Working Directory")
            .description("The directory to use as the current working directory when executing the command")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, true))
            .required(false)
            .build();

    static final PropertyDescriptor IGNORE_STDIN = new PropertyDescriptor.Builder()
            .name("Ignore STDIN")
            .description("If true, the contents of the incoming flowfile will not be passed to the executing command")
            .addValidator(Validator.VALID)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private static final Validator characterValidator = new StandardValidators.StringLengthValidator(1, 1);

    static final PropertyDescriptor ARG_DELIMITER = new PropertyDescriptor.Builder()
            .name("Argument Delimiter")
            .description("Delimiter to use to separate arguments for a command [default: ;]. Must be a single character")
            .addValidator(Validator.VALID)
            .addValidator(characterValidator)
            .required(true)
            .defaultValue(";")
            .build();


    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(EXECUTION_ARGUMENTS);
        props.add(EXECUTION_COMMAND);
        props.add(IGNORE_STDIN);
        props.add(WORKING_DIR);
        props.add(ARG_DELIMITER);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    private ProcessorLog logger;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        logger = getLogger();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
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

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();
        if (null == inputFlowFile) {
            return;
        }

        final ArrayList<String> args = new ArrayList<>();
        final String executeCommand = context.getProperty(EXECUTION_COMMAND).evaluateAttributeExpressions(inputFlowFile).getValue();
        args.add(executeCommand);
        final String commandArguments = context.getProperty(EXECUTION_ARGUMENTS).getValue();
        final boolean ignoreStdin = Boolean.parseBoolean(context.getProperty(IGNORE_STDIN).getValue());
        if (!StringUtils.isBlank(commandArguments)) {
            for (String arg : ArgumentUtils.splitArgs(commandArguments, context.getProperty(ARG_DELIMITER).getValue().charAt(0))) {
                args.add(context.newPropertyValue(arg).evaluateAttributeExpressions(inputFlowFile).getValue());
            }
        }
        final String workingDir = context.getProperty(WORKING_DIR).evaluateAttributeExpressions(inputFlowFile).getValue();

        final ProcessBuilder builder = new ProcessBuilder();

        logger.debug("Executing and waiting for command {} with arguments {}", new Object[]{executeCommand, commandArguments});
        File dir = null;
        if (!StringUtils.isBlank(workingDir)) {
            dir = new File(workingDir);
            if (!dir.exists() && !dir.mkdirs()) {
                logger.warn("Failed to create working directory {}, using current working directory {}", new Object[]{workingDir, System.getProperty("user.dir")});
            }
        }
        final Map<String, String> environment = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), entry.getValue());
            }
        }
        builder.environment().putAll(environment);
        builder.command(args);
        builder.directory(dir);
        builder.redirectInput(Redirect.PIPE);
        builder.redirectOutput(Redirect.PIPE);
        final Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            logger.error("Could not create external process to run command", e);
            throw new ProcessException(e);
        }
        try (final OutputStream pos = process.getOutputStream();
                final InputStream pis = process.getInputStream();
                final InputStream pes = process.getErrorStream();
                final BufferedInputStream bis = new BufferedInputStream(pis);
                final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(pes))) {
            int exitCode = -1;
            final BufferedOutputStream bos = new BufferedOutputStream(pos);
            FlowFile outputStreamFlowFile = session.create(inputFlowFile);
            ProcessStreamWriterCallback callback = new ProcessStreamWriterCallback(ignoreStdin, bos, bis, logger, session, outputStreamFlowFile, process);
            session.read(inputFlowFile, callback);
            outputStreamFlowFile = callback.outputStreamFlowFile;
            exitCode = callback.exitCode;
            logger.debug("Execution complete for command: {}.  Exited with code: {}", new Object[]{executeCommand, exitCode});

            Map<String, String> attributes = new HashMap<>();

            final StringBuilder strBldr = new StringBuilder();
            try {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    strBldr.append(line).append("\n");
                }
            } catch (IOException e) {
                strBldr.append("Unknown...could not read Process's Std Error");
            }
            int length = strBldr.length() > 4000 ? 4000 : strBldr.length();
            attributes.put("execution.error", strBldr.substring(0, length));

            if (exitCode == 0) {
                logger.info("Transferring flow file {} to 'output stream'", new Object[]{outputStreamFlowFile});
            } else {
                logger.error("Transferring flow file {} to 'output stream'. Executable command {} ended in an error: {}",
                        new Object[]{outputStreamFlowFile, executeCommand, strBldr.toString()});
            }

            attributes.put("execution.status", Integer.toString(exitCode));
            attributes.put("execution.command", executeCommand);
            attributes.put("execution.command.args", commandArguments);
            outputStreamFlowFile = session.putAllAttributes(outputStreamFlowFile, attributes);
            session.transfer(outputStreamFlowFile, OUTPUT_STREAM_RELATIONSHIP);
            logger.info("Transferring flow file {} to original", new Object[]{inputFlowFile});
            inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
            session.transfer(inputFlowFile, ORIGINAL_RELATIONSHIP);

        } catch (final IOException ex) {
            // could not close Process related streams
            logger.warn("Problem terminating Process {}", new Object[]{process}, ex);
        } finally {
            process.destroy(); // last ditch effort to clean up that process.
        }
    }

    static class ProcessStreamWriterCallback implements InputStreamCallback {

        final boolean ignoreStdin;
        final OutputStream stdInWritable;
        final InputStream stdOutReadable;
        final ProcessorLog logger;
        final ProcessSession session;
        final Process process;
        FlowFile outputStreamFlowFile;
        int exitCode;

        public ProcessStreamWriterCallback(boolean ignoreStdin, OutputStream stdInWritable, InputStream stdOutReadable,
                                           ProcessorLog logger, ProcessSession session, FlowFile outputStreamFlowFile, Process process) {
            this.ignoreStdin = ignoreStdin;
            this.stdInWritable = stdInWritable;
            this.stdOutReadable = stdOutReadable;
            this.logger = logger;
            this.session = session;
            this.outputStreamFlowFile = outputStreamFlowFile;
            this.process = process;
        }

        @Override
        public void process(final InputStream incomingFlowFileIS) throws IOException {
            outputStreamFlowFile = session.write(outputStreamFlowFile, new OutputStreamCallback() {

                @Override
                public void process(OutputStream out) throws IOException {

                    Thread writerThread = new Thread(new Runnable() {

                        @Override
                        public void run() {
                            if (!ignoreStdin) {
                                try {
                                    StreamUtils.copy(incomingFlowFileIS, stdInWritable);
                                } catch (IOException e) {
                                    logger.error("Failed to write flow file to stdIn due to {}", new Object[]{e}, e);
                                }
                            }
                            // MUST close the output stream to the stdIn so that whatever is reading knows
                            // there is no more data
                            IOUtils.closeQuietly(stdInWritable);
                        }
                    });
                    writerThread.setDaemon(true);
                    writerThread.start();
                    StreamUtils.copy(stdOutReadable, out);
                    try {
                        exitCode = process.waitFor();
                    } catch (InterruptedException e) {
                        logger.warn("Command Execution Process was interrupted", e);
                    }
                }
            });
        }
    }

}

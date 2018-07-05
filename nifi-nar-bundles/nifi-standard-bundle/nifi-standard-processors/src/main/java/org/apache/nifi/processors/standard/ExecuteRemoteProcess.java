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


import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.ArgumentUtils;
import org.apache.nifi.processors.standard.util.SSHExec;
import org.apache.nifi.processors.standard.util.SSHTransfer;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"command", "process", "source", "ssh", "external", "invoke", "script"})
@CapabilityDescription("Runs an operating system command over SSH specified by the user and writes the output of that command to a FlowFile.")

@WritesAttributes({
    @WritesAttribute(attribute = "command", description = "Executed command"),
    @WritesAttribute(attribute = "command.arguments", description = "Arguments of the command")
})
public class ExecuteRemoteProcess extends AbstractProcessor {

    final static String ATTRIBUTE_COMMAND = "command";
    final static String ATTRIBUTE_COMMAND_ARGS = "command.arguments";

    public static final PropertyDescriptor COMMAND = new PropertyDescriptor.Builder()
    .name("Command")
    .description("Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.")
    .required(true)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor COMMAND_ARGUMENTS = new PropertyDescriptor.Builder()
    .name("Command Arguments")
    .description("The arguments to supply to the executable delimited by white space. White space can be escaped by enclosing it in double-quotes.")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
    .name("Working Directory")
    .description("The directory to use as the current working directory when executing the command")
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(StandardValidators.createDirectoryExistsValidator(false, true))
    .required(false)
    .build();

    public static final PropertyDescriptor REDIRECT_ERROR_STREAM = new PropertyDescriptor.Builder()
    .name("Redirect Error Stream")
    .description("If true will redirect any error stream output of the process to the output stream. "
            + "This is particularly helpful for processes which write extensively to the error stream or for troubleshooting.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final Validator characterValidator = new StandardValidators.StringLengthValidator(1, 1);

    static final PropertyDescriptor ARG_DELIMITER = new PropertyDescriptor.Builder()
      .name("Argument Delimiter")
      .description("Delimiter to use to separate arguments for a command [default: space]. Must be a single character.")
      .addValidator(Validator.VALID)
      .addValidator(characterValidator)
      .required(true)
      .defaultValue(" ")
      .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFile containing output is routed to this relationship")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Command ended in error. Error message is written to the FlowFile if available.")
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);

        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(COMMAND);
        properties.add(COMMAND_ARGUMENTS);
        properties.add(REDIRECT_ERROR_STREAM);
        properties.add(WORKING_DIR);
        properties.add(ARG_DELIMITER);
        properties.add(SSHTransfer.HOSTNAME);
        properties.add(SSHTransfer.PORT);
        properties.add(SSHTransfer.USERNAME);
        properties.add(SSHTransfer.PASSWORD);
        properties.add(SSHTransfer.PRIVATE_KEY_PATH);
        properties.add(SSHTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(SSHTransfer.CONNECTION_TIMEOUT);
        properties.add(SSHTransfer.DATA_TIMEOUT);
        properties.add(SSHTransfer.HOST_KEY_FILE);
        properties.add(SSHTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SSHTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SSHTransfer.USE_COMPRESSION);
        properties.add(SSHTransfer.PROXY_CONFIGURATION_SERVICE);
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        final String command = context.getProperty(COMMAND).evaluateAttributeExpressions(flowFile).getValue();
        final String arguments = context.getProperty(COMMAND_ARGUMENTS).isSet()
          ? context.getProperty(COMMAND_ARGUMENTS).evaluateAttributeExpressions(flowFile).getValue()
          : null;

        final String workingDirectory = context.getProperty(WORKING_DIR).evaluateAttributeExpressions(flowFile).getValue();

        // add command and arguments as attribute
        flowFile = session.putAttribute(flowFile, ATTRIBUTE_COMMAND, command);
        if(arguments != null) {
            flowFile = session.putAttribute(flowFile, ATTRIBUTE_COMMAND_ARGS, arguments);
        }

        final List<String> commandStrings = createCommandStrings(context, command, arguments, workingDirectory);
        final String commandString = StringUtils.join(commandStrings, " ");

        // Write to and set the OutputStream for the FlowFile
        // as the delegate for the OuptutStream, then wait until the process finishes
        final FlowFile finalFlowFile = flowFile;
        session.write(finalFlowFile, flowFileOut -> {
            try (final OutputStream out = new BufferedOutputStream(flowFileOut)) {
                SSHExec sshExec = new SSHExec();
                sshExec.setCommand(commandString);

                final String executeOutput = sshExec.execute(SSHTransfer.getSession(context, finalFlowFile));

                out.write(executeOutput.getBytes("UTF-8"));
                out.flush();

                // All was good. Transfer FlowFile.
                session.getProvenanceReporter().create(finalFlowFile, "Created from command: " + commandString);
                getLogger().info("Created {} and routed to success", new Object[] {finalFlowFile});
                session.transfer(finalFlowFile, REL_SUCCESS);
            } catch (IOException e){
                session.transfer(finalFlowFile, REL_FAILURE);
            }
        });

        // Commit the session so that the FlowFile is transferred to the next processor
        session.commit();
    }

    protected List<String> createCommandStrings(final ProcessContext context, final String command, final String arguments, final String workingDirectory) {
        final List<String> args = ArgumentUtils.splitArgs(arguments, context.getProperty(ARG_DELIMITER).getValue().charAt(0));
        final List<String> commandStrings = new ArrayList<>(args.size() + 1);

        if(!StringUtils.isEmpty(workingDirectory)){
            commandStrings.add("cd \"" + workingDirectory + "\" ;");
        }

        commandStrings.add(command);
        commandStrings.addAll(args);
        return commandStrings;
    }
}

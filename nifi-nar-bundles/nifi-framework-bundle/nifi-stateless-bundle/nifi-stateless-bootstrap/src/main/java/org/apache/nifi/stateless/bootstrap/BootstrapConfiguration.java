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

package org.apache.nifi.stateless.bootstrap;

import org.apache.nifi.stateless.config.ParameterOverride;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Pattern;

public class BootstrapConfiguration {
    private static final Pattern PARAMETER_OVERRIDE_PATTERN = Pattern.compile("(?<!\\\\):");
    private static final String PARAMETER_OVERRIDE_FLAG = "-p";
    private static final String RUN_CONTINUOUS_SHORT_FLAG = "-c";
    private static final String RUN_CONTINUOUS_LONG_FLAG = "--continuous";

    private File engineConfigFile = null;
    private File flowDefinitionFile = null;
    private boolean runContinuous = false;
    private List<ParameterOverride> parameterOverrides = new ArrayList<>();

    static void printUsage() {
        System.out.println("Usage:");
        System.out.println();

        System.out.print("java " + RunStatelessFlow.class.getCanonicalName() + " ");
        System.out.print("<options> ");
        System.out.print("<engine configuration properties file> ");
        System.out.print("<flow configuration file>");

        System.out.println();
        System.out.println();
        System.out.println("Options:");
        System.out.println("-p <context name>:<parameter name>:<parameter value>");
        System.out.println("    Specifies a parameter value to use. If the parameter is present in the provided flow configuration file, the value provided here will take precedence.");
        System.out.println("    For example, to specify that the 'Foo' parameter of the Parameter Context with name 'bar' should have a value of 'BAZ', use:");
        System.out.println("    -p bar:Foo:BAZ");
        System.out.println();
        System.out.println("    Multiple Parameters may be specified in this way. For example:");
        System.out.println("    -p bar:Foo:BAZ -p \"My Context:My Parameter:My Value\"");
        System.out.println();
        System.out.println("    If a Parameter name or value or Parameter Context name has a colon in it, it may be escaped using the \\ character:");
        System.out.println("    -p \"My Context:My Parameter:Use ratio of 1\\:1");
        System.out.println();
        System.out.println("-c");
        System.out.println("--continuous");
        System.out.println("    Specifies that the flow should be run continuously. If not specified, the flow will run only a single iteration and stop.");
        System.out.println();
        System.out.println();
    }

    // Must be created via call to #fromCommandLineArgs
    private BootstrapConfiguration() {
    }


    public File getEngineConfigFile() {
        return engineConfigFile;
    }

    public File getFlowDefinitionFile() {
        return flowDefinitionFile;
    }

    public List<ParameterOverride> getParameterOverrides() {
        return parameterOverrides;
    }

    public boolean isRunContinuous() {
        return runContinuous;
    }

    static BootstrapConfiguration fromCommandLineArgs(final String[] args) throws FileNotFoundException {
        // Create configuration and parse arguments.
        final BootstrapConfiguration configuration = new BootstrapConfiguration();
        configuration.parseArguments(args);

        // Validate the configuration
        final File engineConfigFile = configuration.getEngineConfigFile();
        if (engineConfigFile == null) {
            throw new IllegalStateException("Engine Configuration Properties File not specified");
        }
        if (!engineConfigFile.exists()) {
            throw new FileNotFoundException("Could not find properties file " + engineConfigFile.getAbsolutePath());
        }

        final File flowDefinitionFile = configuration.getFlowDefinitionFile();
        if (flowDefinitionFile == null) {
            throw new IllegalStateException("Flow Definition File not specified");
        }
        if (!flowDefinitionFile.exists()) {
            throw new FileNotFoundException("Could not find Flow Definition File " + flowDefinitionFile);
        }

        // Configuration is valid so it can be returned.
        return configuration;
    }

    static boolean isValid(final String[] args) {
        if (args.length < 2) {
            return false;
        }

        try {
            final BootstrapConfiguration configuration = new BootstrapConfiguration();
            configuration.parseArguments(args);
        } catch (final IllegalArgumentException iae) {
            System.out.println(iae.getMessage());
            System.out.println();
            return false;
        }

        return true;
    }

    private Queue<String> toQueue(final String[] args) {
        final Queue<String> queue = new LinkedList<>();
        for (int i=0; i < args.length - 2; i++) {
            queue.offer(args[i]);
        }
        return queue;
    }

    private void parseArguments(final String[] args) {
        if (args.length < 2) {
            throw new IllegalStateException("Engine Configuration Properties File and Flow Definition File must be specified");
        }

        final String engineConfigFilename = args[args.length - 2];
        engineConfigFile = new File(engineConfigFilename);

        final String flowDefinitionFilename = args[args.length - 1];
        flowDefinitionFile = new File(flowDefinitionFilename);

        final Queue<String> argQueue = toQueue(args);

        String arg;
        while ((arg = argQueue.poll()) != null) {
            switch (arg) {
                case PARAMETER_OVERRIDE_FLAG:
                    final String parameterArg = argQueue.poll();
                    if (parameterArg == null) {
                        throw new IllegalArgumentException("Invalid argument: -p argument must be followed by the parameter");
                    }

                    final ParameterOverride override = parseOverride(parameterArg);
                    parameterOverrides.add(override);
                    break;
                case RUN_CONTINUOUS_SHORT_FLAG:
                case RUN_CONTINUOUS_LONG_FLAG:
                    runContinuous = true;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid argument: " + arg);
            }
        }
    }

    private ParameterOverride parseOverride(final String argument) {
        final String[] splits = argument.split(PARAMETER_OVERRIDE_PATTERN.pattern(), 3);

        if (splits.length == 2) {
            final String parameterName = splits[0].replace("\\:", ":");
            final String parameterValue = splits[1].replace("\\:", ":");
            return new ParameterOverride(parameterName, parameterValue);
        } else if (splits.length == 3) {
            final String contextName = splits[0].replace("\\:", ":");
            final String parameterName = splits[1].replace("\\:", ":");
            final String parameterValue = splits[2].replace("\\:", ":");
            return new ParameterOverride(contextName, parameterName, parameterValue);
        }

        throw new IllegalArgumentException("Invalid parameter: " + argument);
    }
}

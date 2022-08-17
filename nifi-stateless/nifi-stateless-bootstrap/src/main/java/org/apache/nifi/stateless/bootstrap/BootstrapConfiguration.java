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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Pattern;

public class BootstrapConfiguration {
    private static final String RUN_CONTINUOUS_ENV_VAR = "NIFI_STATELESS_CONTINUOUS";
    private static final String ENGINE_CONFIG_FILE_ENV_VAR = "NIFI_STATELESS_ENGINE_CONFIG_FILE";
    private static final String FLOW_CONFIGURATION_FILE_ENV_VAR = "NIFI_STATELESS_FLOW_CONFIG_FILE";

    private static final String DEFAULT_ENGINE_CONFIG_FILE = "./conf/stateless.properties";
    private static final String DEFAULT_FLOW_CONFIG_FILE = "./conf/env-flow-config.properties";

    private static final Pattern PARAMETER_OVERRIDE_PATTERN = Pattern.compile( "(?<!\\\\):"  );
    private static final String PARAMETER_OVERRIDE_FLAG = "-p";
    private static final String RUN_CONTINUOUS_SHORT_FLAG = "-c";
    private static final String RUN_CONTINUOUS_LONG_FLAG = "--continuous";
    private static final String ENGINE_CONFIG_SHORT_FLAG = "-e";
    private static final String ENGINE_CONFIG_LONG_FLAG = "--engine-config";
    private static final String FLOW_CONFIGURATION_FILE_SHORT_FLAG = "-f";
    private static final String FLOW_CONFIGURATION_FILE_LONG_FLAG = "--flow-config";

    private File engineConfigFile = null;
    private File flowDefinitionFile = null;
    private boolean runContinuous;
    private List<ParameterOverride> parameterOverrides = new ArrayList<>();

    static void printUsage() {
        System.out.println("Usage:");
        System.out.println();

        System.out.print("java " + RunStatelessFlow.class.getCanonicalName() + " ");
        System.out.println("<options>");

        System.out.println();
        System.out.println();
        System.out.println("Options:");
        System.out.println(PARAMETER_OVERRIDE_FLAG + " <context name>:<parameter name>=<parameter value>");
        System.out.println("    Specifies a parameter value to use. If the parameter is present in the provided flow configuration file, the value provided here will take precedence.");
        System.out.println("    For example, to specify that the 'Foo' parameter of the Parameter Context with name 'bar' should have a value of 'BAZ', use:");
        System.out.println("    -p bar:Foo=BAZ");
        System.out.println();
        System.out.println("    Multiple Parameters may be specified in this way. For example:");
        System.out.println("    -p bar:Foo=BAZ -p \"My Context:My Parameter=My Value\"");
        System.out.println();
        System.out.println("    If a Parameter Context name has a colon in it, it may be escaped using the \\ character. Parameter names and values do not need to be escaped:");
        System.out.println("    -p \"My\\:Context:My:Parameter=Use ratio of 1:1");
        System.out.println();
        System.out.println(RUN_CONTINUOUS_SHORT_FLAG);
        System.out.println(RUN_CONTINUOUS_LONG_FLAG);
        System.out.println("    Specifies that the flow should be run continuously. If not specified, the flow will run only a single iteration and stop ");
        System.out.println("    (unless the " + RUN_CONTINUOUS_LONG_FLAG + " System Property is set to a value of true).");
        System.out.println();
        System.out.println(ENGINE_CONFIG_SHORT_FLAG);
        System.out.println(ENGINE_CONFIG_LONG_FLAG);
        System.out.println("    Specifies the filename of the Stateless Engine Configuration properties file. ");
        System.out.println("    If not specified, the " + ENGINE_CONFIG_FILE_ENV_VAR + " Environment Variable will be inspected.");
        System.out.println("    If that is not set, defaults to " + DEFAULT_ENGINE_CONFIG_FILE);
        System.out.println();
        System.out.println(FLOW_CONFIGURATION_FILE_SHORT_FLAG);
        System.out.println(FLOW_CONFIGURATION_FILE_LONG_FLAG);
        System.out.println("    Specifies the filename of the Flow Configuration properties file.");
        System.out.println("    If not specified, the " + FLOW_CONFIGURATION_FILE_ENV_VAR + " Environment Variable will be inspected.");
        System.out.println("    If that is not set, defaults to " + DEFAULT_FLOW_CONFIG_FILE + ", which sources all configuration from environment variables.");
        System.out.println();
        System.out.println();
    }

    // Must be created via call to #fromCommandLineArgs
    private BootstrapConfiguration() {
        runContinuous = Boolean.parseBoolean(System.getenv(RUN_CONTINUOUS_ENV_VAR));
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

    static BootstrapConfiguration fromCommandLineArgs(final String[] args) {
        // Create configuration and parse arguments.
        final BootstrapConfiguration configuration = new BootstrapConfiguration();
        configuration.parseArguments(args);

        // Configuration is valid so it can be returned.
        return configuration;
    }


    private void parseArguments(final String[] args) {
        // Set defaults for engine config filename
        String engineConfigFilename = System.getenv(ENGINE_CONFIG_FILE_ENV_VAR);
        if (engineConfigFilename == null) {
            engineConfigFilename = DEFAULT_ENGINE_CONFIG_FILE;
        }

        // Set flow configuration filename based on environment variable
        String flowConfigFilename = System.getenv(FLOW_CONFIGURATION_FILE_ENV_VAR);
        if (flowConfigFilename == null) {
            flowConfigFilename = DEFAULT_FLOW_CONFIG_FILE;
        }

        // Parse the command-line arguments
        final Queue<String> argQueue = new LinkedList<>(Arrays.asList(args));

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
                case ENGINE_CONFIG_SHORT_FLAG:
                case ENGINE_CONFIG_LONG_FLAG:
                    engineConfigFilename = argQueue.poll();
                    if (engineConfigFilename == null) {
                        throw new IllegalArgumentException("When specifying flag " + arg + ", next arg must supply the filename of the Engine Configuration file but no arg was given.");
                    }

                    break;
                case FLOW_CONFIGURATION_FILE_SHORT_FLAG:
                case FLOW_CONFIGURATION_FILE_LONG_FLAG:
                    flowConfigFilename = argQueue.poll();
                    if (flowConfigFilename == null) {
                        throw new IllegalArgumentException("When specifying flag " + arg + ", next arg must supply the filename of the Flow Configuration file but no arg was given.");
                    }

                    break;
                case RUN_CONTINUOUS_SHORT_FLAG:
                case RUN_CONTINUOUS_LONG_FLAG:
                    runContinuous = true;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid argument: " + arg);
            }
        }

        // Validate the Engine Configuration File
        engineConfigFile = new File(engineConfigFilename);
        if (!engineConfigFile.exists()) {
            throw new IllegalArgumentException(String.format("Cannot find Engine Configuration File %s - please ensure that the file exists and appropriate permissions are in place for allowing " +
                    "access to the file, or otherwise specify a different filename using the %s or %s command-line argument or by specifying the %s Environment Variable",
                    engineConfigFile.getAbsolutePath(), ENGINE_CONFIG_LONG_FLAG, ENGINE_CONFIG_SHORT_FLAG, ENGINE_CONFIG_FILE_ENV_VAR));
        }

        // Validate the Flow Configuration File
        flowDefinitionFile = new File(flowConfigFilename);
        if (!flowDefinitionFile.exists()) {
            throw new IllegalArgumentException(String.format("Cannot find Flow Configuration File %s - please ensure that the file exists and appropriate permissions are in place for allowing " +
                    "access to the file, or otherwise specify a different filename using the %s or %s command-line argument or by specifying the %s Environment Variable",
                flowDefinitionFile.getAbsolutePath(), FLOW_CONFIGURATION_FILE_LONG_FLAG, FLOW_CONFIGURATION_FILE_SHORT_FLAG, FLOW_CONFIGURATION_FILE_ENV_VAR));
        }
    }

    ParameterOverride parseOverride(final String argument) {
        final String[] nameAndValueSplits = argument.split("=", 2);
        if (nameAndValueSplits.length == 1) {
            throw new IllegalArgumentException("Invalid parameter: argument has no equals sign: " + argument);
        }

        final String contextAndParameterName = nameAndValueSplits[0];
        if (contextAndParameterName.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid parameter: argument has no parameter name: " + argument);
        }

        final String parameterValue = nameAndValueSplits[1];

        final String[] splits = contextAndParameterName.split(PARAMETER_OVERRIDE_PATTERN.pattern(), 2);

        if (splits.length == 1) {
            return new ParameterOverride(contextAndParameterName, parameterValue);
        } else if (splits.length == 2) {
            final String contextName = splits[0].replace("\\:", ":");
            final String parameterName = splits[1];
            return new ParameterOverride(contextName, parameterName, parameterValue);
        }

        throw new IllegalArgumentException("Invalid parameter: " + argument);
    }
}

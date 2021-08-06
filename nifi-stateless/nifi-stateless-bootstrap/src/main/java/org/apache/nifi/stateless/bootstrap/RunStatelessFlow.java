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
import org.apache.nifi.stateless.config.PropertiesFileEngineConfigurationParser;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.StatelessDataflowValidation;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RunStatelessFlow {
    private static final Logger logger = LoggerFactory.getLogger(RunStatelessFlow.class);

    public static void main(final String[] args) throws IOException, StatelessConfigurationException, InterruptedException {
        // Try to parse config from command-line. If there are any failures parsing the configuration, an IllegalArgumentException should be
        // raised detailing the problem. In that case, we will display the message, print the usage, and exit with a non-zero status
        final BootstrapConfiguration bootstrapConfiguration;
        try {
            bootstrapConfiguration = BootstrapConfiguration.fromCommandLineArgs(args);
        } catch (final IllegalArgumentException iae) {
            System.out.println("* " + iae.getMessage()); // There may be a lot of output to the console; Use an astrisk at the front just to make this line stand out.
            System.out.println();
            BootstrapConfiguration.printUsage();
            System.exit(1);
            return;
        }

        final PropertiesFileEngineConfigurationParser engineConfigParser = new PropertiesFileEngineConfigurationParser();
        final StatelessEngineConfiguration engineConfiguration = engineConfigParser.parseEngineConfiguration(bootstrapConfiguration.getEngineConfigFile());

        final StatelessDataflow dataflow = createDataflow(engineConfiguration, bootstrapConfiguration.getFlowDefinitionFile(), bootstrapConfiguration.getParameterOverrides());

        try {
            if (bootstrapConfiguration.isRunContinuous()) {
                triggerContinuously(dataflow);
            } else {
                triggerOnce(dataflow);
            }
        } finally {
            dataflow.shutdown();
        }
    }

    private static void triggerContinuously(final StatelessDataflow dataflow) throws InterruptedException {
        while (true) {
            try {
                triggerOnce(dataflow);
            } catch (final InterruptedException ie) {
                throw ie;
            } catch (final Exception e) {
                logger.error("Failed to run dataflow", e);
            }
        }
    }

    private static void triggerOnce(final StatelessDataflow dataflow) throws InterruptedException {
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        result.acknowledge();
    }

    public static StatelessDataflow createDataflow(final StatelessEngineConfiguration engineConfiguration, final File flowDefinitionFile, final List<ParameterOverride> parameterOverrides)
                throws IOException, StatelessConfigurationException {
        final long initializeStart = System.currentTimeMillis();

        final StatelessBootstrap bootstrap = StatelessBootstrap.bootstrap(engineConfiguration);
        final DataflowDefinition<?> dataflowDefinition = bootstrap.parseDataflowDefinition(flowDefinitionFile, parameterOverrides);

        final StatelessDataflow dataflow = bootstrap.createDataflow(dataflowDefinition);
        dataflow.initialize();

        final StatelessDataflowValidation validation = dataflow.performValidation();
        if (!validation.isValid()) {
            logger.error(validation.toString());
            throw new IllegalStateException("Dataflow is not valid");
        }

        final long initializeMillis = System.currentTimeMillis() - initializeStart;
        logger.info("Initialized Stateless NiFi in {} millis", initializeMillis);

        return dataflow;
    }
}

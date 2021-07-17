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

package org.apache.nifi.stateless;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.stateless.bootstrap.StatelessBootstrap;
import org.apache.nifi.stateless.config.ExtensionClientDefinition;
import org.apache.nifi.stateless.config.ParameterContextDefinition;
import org.apache.nifi.stateless.config.ParameterProviderDefinition;
import org.apache.nifi.stateless.config.ReportingTaskDefinition;
import org.apache.nifi.stateless.config.SslContextDefinition;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StatelessSystemIT {
    private final List<StatelessDataflow> createdFlows = new ArrayList<>();

    // We reference version 1.13.0 here, but the version isn't really relevant. Because there will only be a single artifact of name "nifi-system-test-extensions-nar" the framework will end
    // up finding a "compatible bundle" and using that, regardless of the specified version.
    protected static final Bundle SYSTEM_TEST_EXTENSIONS_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "1.13.0-SNAPSHOT");

    @Rule
    public TestName name = new TestName();

    @Rule
    public Timeout defaultTimeout = new Timeout(30, TimeUnit.MINUTES);


    @Before
    public void clearFlows() {
        createdFlows.clear();
    }

    @After
    public void shutdownFlows() {
        createdFlows.forEach(StatelessDataflow::shutdown);
    }

    protected StatelessEngineConfiguration getEngineConfiguration() {
        return new StatelessEngineConfiguration() {
            @Override
            public File getWorkingDirectory() {
                return new File("target/work");
            }

            @Override
            public File getNarDirectory() {
                return new File("target/nifi-stateless-assembly/nars");
            }

            @Override
            public File getExtensionsDirectory() {
                return new File("target/nifi-stateless-assembly/extensions");
            }

            @Override
            public File getKrb5File() {
                return new File("/etc/krb5.conf");
            }

            @Override
            public SslContextDefinition getSslContext() {
                return null;
            }

            @Override
            public String getSensitivePropsKey() {
                return "nifi-stateless";
            }

            @Override
            public List<ExtensionClientDefinition> getExtensionClients() {
                return Collections.emptyList();
            }
        };
    }

    protected StatelessDataflow loadDataflow(final File versionedFlowSnapshot, final List<ParameterContextDefinition> parameterContexts) throws IOException, StatelessConfigurationException {
        final ObjectMapper objectMapper = new ObjectMapper();

        final VersionedFlowSnapshot snapshot;
        try (final InputStream fis = new FileInputStream(versionedFlowSnapshot)) {
            snapshot = objectMapper.readValue(fis, VersionedFlowSnapshot.class);
        }

        return loadDataflow(snapshot, parameterContexts);
    }

    protected StatelessDataflow loadDataflow(final VersionedFlowSnapshot versionedFlowSnapshot) throws IOException, StatelessConfigurationException {
        return loadDataflow(versionedFlowSnapshot, Collections.emptyList());
    }

    protected StatelessDataflow loadDataflow(final VersionedFlowSnapshot versionedFlowSnapshot, final List<ParameterContextDefinition> parameterContexts)
        throws IOException, StatelessConfigurationException {
        return loadDataflow(versionedFlowSnapshot, parameterContexts, Collections.emptySet());
    }

    protected StatelessDataflow loadDataflow(final VersionedFlowSnapshot versionedFlowSnapshot, final List<ParameterContextDefinition> parameterContexts, final Set<String> failurePortNames)
                    throws IOException, StatelessConfigurationException {
        return loadDataflow(versionedFlowSnapshot, parameterContexts, failurePortNames, TransactionThresholds.SINGLE_FLOWFILE);
    }

    protected StatelessDataflow loadDataflow(final VersionedFlowSnapshot versionedFlowSnapshot, final List<ParameterContextDefinition> parameterContexts, final Set<String> failurePortNames,
                                             final TransactionThresholds transactionThresholds) throws IOException, StatelessConfigurationException {
        return loadDataflow(versionedFlowSnapshot, parameterContexts, Collections.emptyList(), failurePortNames, transactionThresholds);
    }

    protected StatelessDataflow loadDataflow(final VersionedFlowSnapshot versionedFlowSnapshot, final List<ParameterContextDefinition> parameterContexts,
                                             final List<ParameterProviderDefinition> parameterProviderDefinitions, final Set<String> failurePortNames,
                                             final TransactionThresholds transactionThresholds) throws IOException, StatelessConfigurationException {

        final DataflowDefinition<VersionedFlowSnapshot> dataflowDefinition = new DataflowDefinition<VersionedFlowSnapshot>() {
            @Override
            public VersionedFlowSnapshot getFlowSnapshot() {
                return versionedFlowSnapshot;
            }

            @Override
            public String getFlowName() {
                return null;
            }

            @Override
            public Set<String> getFailurePortNames() {
                return failurePortNames;
            }

            @Override
            public List<ParameterContextDefinition> getParameterContexts() {
                return parameterContexts;
            }

            @Override
            public List<ReportingTaskDefinition> getReportingTaskDefinitions() {
            return Collections.emptyList();
        }

            @Override
            public List<ParameterProviderDefinition> getParameterProviderDefinitions() {
                return parameterProviderDefinitions;
            }

            @Override
            public TransactionThresholds getTransactionThresholds() {
                return transactionThresholds;
            }
        };

        final StatelessBootstrap bootstrap = StatelessBootstrap.bootstrap(getEngineConfiguration());
        final StatelessDataflow dataflow = bootstrap.createDataflow(dataflowDefinition);
        dataflow.initialize();

        createdFlows.add(dataflow);
        return dataflow;
    }

    protected String getTestName() {
        return name.getMethodName();
    }
}

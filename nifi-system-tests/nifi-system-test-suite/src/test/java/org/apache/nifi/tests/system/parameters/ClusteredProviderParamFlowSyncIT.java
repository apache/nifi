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
package org.apache.nifi.tests.system.parameters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.parameter.ParameterSensitivity;
import org.apache.nifi.parameter.StandardParameterProviderConfiguration;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterGroupConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderApplyParametersRequestEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces the flow-synchronization failure that occurs when a provider-backed Parameter Context
 * contains a parameter flagged as user-entered (provided=false) in the serialized flow. A provider-backed
 * context can never hold such a parameter as a live object (it is rejected at every write path), but the
 * provided flag on a serialized VersionedParameter is set independently of those guards. When a node loads
 * or reconciles a flow whose provider-backed context carries a provided=false parameter, the flow
 * synchronizer attempts a manual parameter update, which the Parameter Context rejects, and the node fails
 * to bring its flow up.
 */
public class ClusteredProviderParamFlowSyncIT extends NiFiSystemIT {

    private static final String CONTEXT_NAME = "provider-backed-context";
    private static final String GROUP_NAME = "Parameters";
    private static final String PARAM_NAME = "db.host";
    private static final String GUARD_MESSAGE = "cannot be manually updated because they are provided by Parameter Provider";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        // This test intentionally corrupts the persisted flow, so the environment must not be reused.
        return true;
    }

    @Test
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    public void testProviderBackedContextWithNonProvidedParameterReconcilesOnFlowSync() throws Exception {
        // 1. Create a Parameter Provider and a provider-backed Parameter Context, and apply provided parameters
        //    so that the parameter is provider-supplied (provided=true) on both nodes.
        final ParameterProviderEntity provider = getClientUtil().createParameterProvider("PropertiesParameterProvider");
        final Map<String, String> providerProps = new HashMap<>();
        providerProps.put("parameters", PARAM_NAME + "=localhost");
        getClientUtil().updateParameterProviderProperties(provider, providerProps);

        final ParameterContextEntity contextEntity = getClientUtil().createParameterContextEntity(
                CONTEXT_NAME, null, Collections.emptySet(), Collections.emptyList(),
                new StandardParameterProviderConfiguration(provider.getId(), GROUP_NAME, true));
        final ParameterContextEntity createdContext = getNifiClient().getParamContextClient().createParamContext(contextEntity);
        getClientUtil().setParameterContext("root", createdContext);

        final ParameterGroupConfigurationEntity groupConfig = new ParameterGroupConfigurationEntity();
        groupConfig.setSynchronized(true);
        groupConfig.setGroupName(GROUP_NAME);
        groupConfig.setParameterContextName(CONTEXT_NAME);
        final Map<String, ParameterSensitivity> sensitivities = new HashMap<>();
        sensitivities.put(PARAM_NAME, ParameterSensitivity.NON_SENSITIVE);
        groupConfig.setParameterSensitivities(sensitivities);

        getClientUtil().fetchParameters(provider);
        final ParameterProviderApplyParametersRequestEntity applyRequest =
                getClientUtil().applyParameters(provider, List.of(groupConfig));
        getClientUtil().waitForParameterProviderApplicationRequestToComplete(
                applyRequest.getRequest().getParameterProvider().getId(), applyRequest.getRequest().getRequestId());

        waitForAllNodesConnected();

        // 2. Stop the whole cluster and corrupt the persisted flow on BOTH nodes: flip the provider-backed
        //    context's parameter to provided=false with a divergent value. Corrupting both nodes ensures the
        //    elected cluster flow carries the contradiction regardless of which node wins flow election.
        final NiFiInstance node1 = getNiFiInstance().getNodeInstance(1);
        final NiFiInstance node2 = getNiFiInstance().getNodeInstance(2);
        node1.stop();
        node2.stop();

        corruptProviderBackedParameter(node1);
        corruptProviderBackedParameter(node2);

        // 3. Restart both nodes. With the fix, each node recognizes the context is provider-backed and reconciles
        //    it as provider-managed (re-sourcing values from the Parameter Provider) rather than attempting a
        //    manual update, so both nodes rejoin the cluster cleanly.
        node1.start(true);
        node2.start(true);

        // 4. The cluster forms: both nodes connect (this would time out if reconciliation still failed the guard),
        //    and neither node logs the provider-backed-context guard failure or a flow mismatch.
        waitForAllNodesConnected();

        final List<NiFiInstance> nodes = List.of(node1, node2);
        assertNoNodeLogContains(nodes, GUARD_MESSAGE);
        assertNoNodeLogContains(nodes, "did not Match Cluster Flow");

        // 5. The reconciled parameter resolves to the provider-supplied value (re-sourced from the Parameter
        //    Provider), not the corrupted user-entered value or null. This makes the "reconciled as
        //    provider-managed" guarantee explicit rather than only inferring it from the absent guard message.
        final ParameterContextEntity reconciledContext =
                getNifiClient().getParamContextClient().getParamContext(createdContext.getId(), false);
        final String reconciledValue = reconciledContext.getComponent().getParameters().stream()
                .filter(entity -> PARAM_NAME.equals(entity.getParameter().getName()))
                .map(entity -> entity.getParameter().getValue())
                .findFirst()
                .orElse(null);
        assertEquals("localhost", reconciledValue,
                "Parameter [" + PARAM_NAME + "] must resolve to the provider-supplied value after reconciliation");
    }

    private void corruptProviderBackedParameter(final NiFiInstance node) throws IOException {
        final File flowFile = new File(new File(node.getInstanceDirectory(), "conf"), "flow.json.gz");

        final byte[] bytes;
        try (final InputStream fis = new FileInputStream(flowFile);
             final InputStream in = new GZIPInputStream(fis);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            StreamUtils.copy(in, baos);
            bytes = baos.toByteArray();
        }

        final VersionedDataflow dataflow;
        try (final InputStream in = new ByteArrayInputStream(bytes)) {
            dataflow = objectMapper.readValue(in, VersionedDataflow.class);
        }

        boolean mutated = false;
        if (dataflow.getParameterContexts() != null) {
            for (final VersionedParameterContext context : dataflow.getParameterContexts()) {
                if (!CONTEXT_NAME.equals(context.getName()) || context.getParameters() == null) {
                    continue;
                }
                for (final VersionedParameter parameter : context.getParameters()) {
                    if (PARAM_NAME.equals(parameter.getName())) {
                        parameter.setProvided(false);
                        parameter.setValue("user-entered-divergent-value");
                        mutated = true;
                    }
                }
            }
        }
        assertTrue(mutated, "Expected provider-backed parameter [" + PARAM_NAME + "] in the persisted flow to corrupt");

        try (final OutputStream fos = new FileOutputStream(flowFile);
             final OutputStream gzipOut = new GZIPOutputStream(fos)) {
            objectMapper.writeValue(gzipOut, dataflow);
        }
    }

    private void assertNoNodeLogContains(final List<NiFiInstance> nodes, final String unexpected) throws IOException {
        for (final NiFiInstance node : nodes) {
            final File logFile = new File(new File(node.getInstanceDirectory(), "logs"), "nifi-app.log");
            if (logFile.exists()) {
                assertFalse(Files.readString(logFile.toPath()).contains(unexpected),
                        "Node log unexpectedly contains: " + unexpected);
            }
        }
    }
}

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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ParameterContextIT extends NiFiSystemIT {
    @Test
    public void testCreateParameterContext() throws NiFiClientException, IOException {
        final Set<ParameterEntity> parameterEntities = new HashSet<>();
        parameterEntities.add(createParameterEntity("foo", null, false, "bar"));
        final ParameterContextEntity entity = createParameterContextEntity(getTestName(), "System Test for verifying creation of Parameter Context", parameterEntities);

        final ParamContextClient paramContextClient = getNifiClient().getParamContextClient();
        final ParameterContextEntity returned = paramContextClient.createParamContext(entity);
        assertSingleFooCreation(returned);

        final String contextId = returned.getId();
        final ParameterContextEntity fetched = paramContextClient.getParamContext(contextId);
        assertSingleFooCreation(fetched);
    }

    private void assertSingleFooCreation(final ParameterContextEntity entity) {
        final ParameterContextDTO returnedDto = entity.getComponent();
        assertEquals(getTestName(), returnedDto.getName());

        final Set<ParameterEntity> returnedParamEntities = returnedDto.getParameters();
        assertEquals(1, returnedParamEntities.size());
        final ParameterDTO returnedParamDto = returnedParamEntities.iterator().next().getParameter();
        assertEquals("foo", returnedParamDto.getName());
        assertNull(returnedParamDto.getDescription());
        assertSame(Boolean.FALSE,returnedParamDto.getSensitive());
        assertEquals("bar", returnedParamDto.getValue());
    }

    @Test
    public void testSensitiveParametersNotReturned() throws NiFiClientException, IOException {
        final Set<ParameterEntity> parameterEntities = new HashSet<>();
        parameterEntities.add(createParameterEntity("foo", null, true, "bar"));

        final ParameterContextEntity entity = createParameterContextEntity(getTestName(), null, parameterEntities);
        final ParamContextClient paramContextClient = getNifiClient().getParamContextClient();
        final ParameterContextEntity returned = paramContextClient.createParamContext(entity);
        assertSensitiveParametersNotReturned(returned);

        final String contextId = returned.getId();
        final ParameterContextEntity fetched = paramContextClient.getParamContext(contextId);
        assertSensitiveParametersNotReturned(fetched);
    }

    private void assertSensitiveParametersNotReturned(final ParameterContextEntity entity) {
        final ParameterContextDTO dto = entity.getComponent();
        assertEquals(getTestName(), dto.getName());

        final Set<ParameterEntity> returnedParamEntities = dto.getParameters();
        assertEquals(1, returnedParamEntities.size());

        final ParameterDTO returnedParamDto = returnedParamEntities.iterator().next().getParameter();
        assertEquals("foo", returnedParamDto.getName());
        assertNull(returnedParamDto.getDescription());
        assertSame(Boolean.TRUE,returnedParamDto.getSensitive());
        assertEquals("********", returnedParamDto.getValue());
    }


    @Test
    public void testAddingMissingParameterMakesProcessorValid() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity createdProcessorEntity = createProcessor(TEST_PROCESSORS_PACKAGE + ".CountEvents", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());
        final String processorId = createdProcessorEntity.getId();

        final ProcessorConfigDTO config = createdProcessorEntity.getComponent().getConfig();
        config.setProperties(Collections.singletonMap("Name", "#{foo}"));
        getNifiClient().getProcessorClient().updateProcessor(createdProcessorEntity);

        waitForInvalidProcessor(processorId);

        final Set<ParameterEntity> parameters = new HashSet<>();
        parameters.add(createParameterEntity("foo", null, false, "bar"));
        final ParameterContextEntity contextEntity = createParameterContextEntity(getTestName(), null, parameters);
        final ParameterContextEntity createdContextEntity = getNifiClient().getParamContextClient().createParamContext(contextEntity);

        setParameterContext("root", createdContextEntity);
        waitForValidProcessor(processorId);
    }

    @Test
    public void testValidationWithRequiredPropertiesAndDefault() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("File Size", "#{foo}"));
        getClientUtil().setAutoTerminatedRelationships(generate, "success");

        final String processorId = generate.getId();

        waitForInvalidProcessor(processorId);

        final ParameterEntity fooKB = createParameterEntity("foo", null, false, "1 KB");
        final Set<ParameterEntity> parameters = new HashSet<>();
        parameters.add(fooKB);
        final ParameterContextEntity contextEntity = createParameterContextEntity(getTestName(), null, parameters);
        final ParameterContextEntity createdContextEntity = getNifiClient().getParamContextClient().createParamContext(contextEntity);

        setParameterContext("root", createdContextEntity);
        waitForValidProcessor(processorId);

        final ParameterEntity fooNull = createParameterEntity("foo", null, false, null);
        createdContextEntity.getComponent().setParameters(Collections.singleton(fooNull));
        getNifiClient().getParamContextClient().updateParamContext(createdContextEntity);
        // Should remain valid because property has a default.
        waitForValidProcessor(processorId);
    }

    @Test(timeout=30000)
    public void testValidationWithRequiredPropertiesAndNoDefault() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("DependOnProperties");
        final Map<String, String> properties = new HashMap<>();
        properties.put("Always Required", "#{foo}");
        properties.put("Required If Always Required Is Bar Or Baz", "15");
        getClientUtil().updateProcessorProperties(generate, properties);
        final String processorId = generate.getId();

        waitForInvalidProcessor(processorId);

        final ParameterEntity fooBar = createParameterEntity("foo", null, false, "bar");
        final Set<ParameterEntity> parameters = new HashSet<>();
        parameters.add(fooBar);
        final ParameterContextEntity contextEntity = createParameterContextEntity(getTestName(), null, parameters);
        final ParameterContextEntity createdContextEntity = getNifiClient().getParamContextClient().createParamContext(contextEntity);

        setParameterContext("root", createdContextEntity);
        waitForValidProcessor(processorId);

        // Create a Parameter that sets the 'foo' value to null and denote that the parameter's value should be explicitly removed.
        final ParameterEntity fooNull = createParameterEntity("foo", null, false, null);
        fooNull.getParameter().setValueRemoved(true);
        createdContextEntity.getComponent().setParameters(Collections.singleton(fooNull));
        getNifiClient().getParamContextClient().updateParamContext(createdContextEntity);

        // Should become invalid because property is required and has no default
        waitForInvalidProcessor(processorId);
    }


    @Test
    public void testParametersReferencingEL() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("a", "1"));
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 min");

        final ProcessorEntity evaluate = getClientUtil().createProcessor("EvaluatePropertiesWithDifferentELScopes");
        final Map<String, String> evaluateProperties = new HashMap<>();
        evaluateProperties.put("FlowFile Context", "#{A}");
        evaluateProperties.put("Variable Registry Context", "#{A Replace With 5}");
        evaluateProperties.put("Expression Language Not Evaluated", "#{Eleven A}");
        getClientUtil().updateProcessorProperties(evaluate, evaluateProperties);

        getClientUtil().createConnection(generate, evaluate, "success");
        getClientUtil().setAutoTerminatedRelationships(evaluate, "success");

        final Set<ParameterEntity> parameters = new HashSet<>();
        parameters.add(createParameterEntity("A", null, false, "${a}"));
        parameters.add(createParameterEntity("A Replace With 5", null, false, "${a:replaceNull(5)}"));
        parameters.add(createParameterEntity("Eleven A", null, false, "11${a}"));
        final ParameterContextEntity contextEntity = createParameterContextEntity(getTestName(), null, parameters);
        final ParameterContextEntity createdContextEntity = getNifiClient().getParamContextClient().createParamContext(contextEntity);

        setParameterContext("root", createdContextEntity);

        waitForValidProcessor(generate.getId());
        waitForValidProcessor(evaluate.getId());

        getClientUtil().startProcessGroupComponents("root");

        waitFor(() -> {
            try {
                return getClientUtil().getCountersAsMap(evaluate.getId()).get("flowfile") == getNumberOfNodes();
            } catch (final Exception e) {
                return false;
            }
        });

        final Map<String, Long> counters = getClientUtil().getCountersAsMap(evaluate.getId());
        assertEquals(getNumberOfNodes(), counters.get("flowfile").longValue());
        assertEquals(5L * getNumberOfNodes(), counters.get("variable.registry").longValue()); // Since no value present in variable registry, will replace null with 5.
        assertFalse(counters.containsKey("no.el.evaluation")); // Should not be evaluated

        // Update parameters so that Eleven A has the value 11 without evaluating EL.
        final Set<ParameterEntity> updatedParameters = new HashSet<>();
        updatedParameters.add(createParameterEntity("A", null, false, "${a}"));
        updatedParameters.add(createParameterEntity("A Replace With 5", null, false, "${a:replaceNull(5)}"));
        updatedParameters.add(createParameterEntity("Eleven A", null, false, "11"));
        final ParameterContextEntity updatedContextEntity = createParameterContextEntity(getTestName() + "-2", null, updatedParameters);
        final ParameterContextEntity secondContextEntity = getNifiClient().getParamContextClient().createParamContext(updatedContextEntity);

        // Stop process group so we can change the Parameter Context, then change the context and restart.
        getClientUtil().stopProcessGroupComponents("root");
        setParameterContext("root", secondContextEntity);
        getClientUtil().startProcessGroupComponents("root");

        // Wait for the 'no.el.evaluation' counter to be set
        waitFor(() -> {
            try {
                return getClientUtil().getCountersAsMap(evaluate.getId()).get("no.el.evaluation") == 11 * getNumberOfNodes();
            } catch (final Exception e) {
                return false;
            }
        });
    }

    @Test
    public void testParameterWithOptionalProperty() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 min");

        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", "#{Text}");
        generateProperties.put("File Size", "1 KB");
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        final ProcessorEntity writeFile = getClientUtil().createProcessor("WriteToFile");
        final File file = new File("target/testParameterWithOptionalProperty.txt");
        getClientUtil().updateProcessorProperties(writeFile, Collections.singletonMap("Filename", file.getAbsolutePath()));

        getClientUtil().createConnection(generate, writeFile, "success");
        getClientUtil().setAutoTerminatedRelationships(writeFile, new HashSet<>(Arrays.asList("success", "failure")));

        final Set<ParameterEntity> parameters = new HashSet<>();
        final ParameterContextEntity contextEntity = createParameterContextEntity(getTestName(), null, parameters);
        final ParameterContextEntity createdContextEntity = getNifiClient().getParamContextClient().createParamContext(contextEntity);

        setParameterContext("root", createdContextEntity);

        // Processor should be invalid because it references a Parameter (Text) that does not exist
        waitForInvalidProcessor(generate.getId());

        // Update the Parameter Context to add new parameter but with null value.
        final ParameterEntity nullText = createParameterEntity("Text", "Text", false, null);
        createdContextEntity.getComponent().getParameters().add(nullText);
        getNifiClient().getParamContextClient().updateParamContext(createdContextEntity);

        waitForValidProcessor(generate.getId());

        getClientUtil().startProcessGroupComponents("root");

        // Ensure that the file is written with a file size of 1 KB
        waitFor(() -> {
            try {
                return file.exists() && file.length() == 1024;
            } catch (final Exception e) {
                return false;
            }
        });

        // Update Parameter to have a specific value
        createdContextEntity.getComponent().getParameters().remove(nullText);
        final String customText = "Some Custom Text";
        createdContextEntity.getComponent().getParameters().add(createParameterEntity("Text", "Text", false, customText));

        getNifiClient().getParamContextClient().updateParamContext(createdContextEntity);

        // Wait for file to be written out
        waitFor(() -> {
            try {
                final boolean correctSize = file.exists() && file.length() == customText.length();
                if (!correctSize) {
                    return false;
                }

                final List<String> lines = Files.readAllLines(file.toPath());
                if (lines.size() != 1) {
                    return false;
                }

                return customText.equals(lines.get(0));
            } catch (final Exception e) {
                return false;
            }
        });

    }

    @Test
    public void testProcessorStartedAfterLongValidationPeriod() throws NiFiClientException, IOException, InterruptedException {
        final ParameterContextEntity createdContextEntity = createParameterContext("sleep", "6 secs");

        // Set the Parameter Context on the root Process Group
        setParameterContext("root", createdContextEntity);

        // Create a Processor and update it to reference Parameter "name"
        ProcessorEntity processorEntity = createProcessor(TEST_PROCESSORS_PACKAGE + ".Sleep", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());
        final String processorId = processorEntity.getId();

        // Update processor to reference Parameter "name"
        final ProcessorConfigDTO config = processorEntity.getComponent().getConfig();
        config.setProperties(Collections.singletonMap("Validate Sleep Time", "#{sleep}"));
        config.setAutoTerminatedRelationships(Collections.singleton("success"));
        getNifiClient().getProcessorClient().updateProcessor(processorEntity);

        waitForValidProcessor(processorId);

        // Start Processors
        getNifiClient().getProcessorClient().startProcessor(processorId, processorEntity.getRevision().getClientId(), 1);

        try {
            // Update Parameter Context to a long validation time.
            final ParameterContextUpdateRequestEntity updateRequestEntity = updateParameterContext(createdContextEntity, "sleep", "6 sec");

            final Set<AffectedComponentEntity> affectedComponents = updateRequestEntity.getRequest().getReferencingComponents();
            assertEquals(1, affectedComponents.size());
            final String affectedComponentId = affectedComponents.iterator().next().getId();
            assertEquals(processorId, affectedComponentId);

            getClientUtil().waitForParameterContextRequestToComplete(createdContextEntity.getId(), updateRequestEntity.getRequest().getRequestId());

            waitForRunningProcessor(processorId);
        } finally {
            // Ensure that we stop the processor so that other tests are allowed to change the Parameter Context, etc.
            getNifiClient().getProcessorClient().stopProcessor(processorId, processorEntity.getRevision().getClientId(), 2);
            waitForStoppedProcessor(processorId);

            getNifiClient().getProcessorClient().deleteProcessor(processorId, processorEntity.getRevision().getClientId(), 3);
        }
    }

    @Test
    public void testProcessorRestartedAfterLongDependentServiceValidationPeriod() throws NiFiClientException, IOException, InterruptedException {
        final ParameterContextEntity createdContextEntity = createParameterContext("sleep", "0 secs");

        // Set the Parameter Context on the root Process Group
        setParameterContext("root", createdContextEntity);

        final ControllerServiceEntity serviceEntity = createControllerService(TEST_CS_PACKAGE + ".StandardSleepService", "root", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());
        final String serviceId = serviceEntity.getId();

        // Set service's sleep time to the parameter.
        serviceEntity.getComponent().setProperties(Collections.singletonMap("Validate Sleep Time", "#{sleep}"));
        getNifiClient().getControllerServicesClient().updateControllerService(serviceEntity);
        getClientUtil().enableControllerService(serviceEntity);

        try {
            // Create a Processor
            ProcessorEntity processorEntity = createProcessor(TEST_PROCESSORS_PACKAGE + ".Sleep", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());
            final String processorId = processorEntity.getId();

            processorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("Sleep Service", serviceId));
            processorEntity.getComponent().getConfig().setAutoTerminatedRelationships(Collections.singleton("success"));

            getNifiClient().getProcessorClient().updateProcessor(processorEntity);
            getNifiClient().getProcessorClient().startProcessor(processorId, processorEntity.getRevision().getClientId(), 1L);

            try {
                final ParameterContextUpdateRequestEntity requestEntity = updateParameterContext(createdContextEntity, "sleep", "6 secs");
                final Set<AffectedComponentEntity> affectedComponentEntities = requestEntity.getRequest().getReferencingComponents();
                assertEquals(2, affectedComponentEntities.size());

                final Set<String> affectedComponentIds = affectedComponentEntities.stream()
                    .map(AffectedComponentEntity::getId)
                    .collect(Collectors.toSet());

                assertTrue(affectedComponentIds.contains(serviceId));
                assertTrue(affectedComponentIds.contains(processorId));

                waitForRunningProcessor(processorId);
            } finally {
                getNifiClient().getProcessorClient().stopProcessor(processorId, processorEntity.getRevision().getClientId(), 1L);
                waitForStoppedProcessor(processorId);
                getNifiClient().getProcessorClient().deleteProcessor(processorId, processorEntity.getRevision().getClientId(), 3);
            }
        } finally {
            getClientUtil().disableControllerService(serviceEntity);
            getNifiClient().getControllerServicesClient().deleteControllerService(serviceEntity);
        }
    }

    @Test
    public void testParamChangeWhileReferencingControllerServiceEnabling() throws NiFiClientException, IOException, InterruptedException {
        final ParameterContextEntity createdContextEntity = createParameterContext("sleep", "7 sec");

        // Set the Parameter Context on the root Process Group
        setParameterContext("root", createdContextEntity);

        final ControllerServiceEntity serviceEntity = createControllerService(TEST_CS_PACKAGE + ".StandardSleepService", "root", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());

        // Set service's sleep time to the parameter.
        serviceEntity.getComponent().setProperties(Collections.singletonMap("@OnEnablend Sleep Time", "#{sleep}"));
        getNifiClient().getControllerServicesClient().updateControllerService(serviceEntity);

        // Enable the service. It should take 7 seconds for the service to fully enable.
        getClientUtil().enableControllerService(serviceEntity);

        // Wait for the service to reach of state of ENABLING but not enabled. We want to change the parameter that it references while it's enabling.
        getClientUtil().waitForControllerServiceState(serviceEntity.getParentGroupId(), "ENABLING", Collections.emptyList());

        // While the service is enabling, change the parameter
        final ParameterContextUpdateRequestEntity paramUpdateRequestEntity = updateParameterContext(createdContextEntity, "sleep", "1 sec");

        // Wait for the update to complete
        getClientUtil().waitForParameterContextRequestToComplete(createdContextEntity.getId(), paramUpdateRequestEntity.getRequest().getRequestId());
    }

    @Test
    public void testParamChangeWhileReferencingProcessorStartingButInvalid() throws NiFiClientException, IOException, InterruptedException {
        final ParameterContextEntity contextEntity = createParameterContext("clone", "true");

        // Set the Parameter Context on the root Process Group
        setParameterContext("root", contextEntity);

        // Create simple dataflow: GenerateFlowFile -> SplitByLine -> <auto-terminate>
        // Set SplitByLine to use a parameter for the "Use Clone" property such that it's valid.
        ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity splitByLine = getClientUtil().createProcessor("SplitByLine");

        getClientUtil().updateProcessorProperties(splitByLine, Collections.singletonMap("Use Clone", "#{clone}"));
        getClientUtil().setAutoTerminatedRelationships(splitByLine, Collections.singleton("success"));
        getClientUtil().createConnection(generate, splitByLine, "success");

        getNifiClient().getProcessorClient().startProcessor(splitByLine);

        // Change parameter to an invalid value. This will result in the processor being stopped, becoming invalid, and then being transitioned to a 'starting' state while invalid.
        final ParameterContextUpdateRequestEntity updateToInvalidRequestEntity = updateParameterContext(contextEntity, "clone", "invalid");
        getClientUtil().waitForParameterContextRequestToComplete(contextEntity.getId(), updateToInvalidRequestEntity.getRequest().getRequestId());

        // Change back to a valid value and wait for the update to complete
        final ParameterContextUpdateRequestEntity updateToValidRequestEntity = updateParameterContext(contextEntity, "clone", "true");
        getClientUtil().waitForParameterContextRequestToComplete(contextEntity.getId(), updateToValidRequestEntity.getRequest().getRequestId());
    }

    @Test
    public void testProcessorRestartedWhenParameterChanged() throws NiFiClientException, IOException, InterruptedException {
        testProcessorRestartedWhenParameterChanged("#{name}");
    }

    @Test
    public void testProcessorRestartedWhenParameterChangedWhenReferencedThroughEL() throws NiFiClientException, IOException, InterruptedException {
        testProcessorRestartedWhenParameterChanged("${'hello':equals(#{name})}");
    }

    private void testProcessorRestartedWhenParameterChanged(final String propertyValue) throws NiFiClientException, IOException, InterruptedException {
        final Set<ParameterEntity> parameters = new HashSet<>();
        parameters.add(createParameterEntity("name", null, false, "bar"));
        final ParameterContextEntity contextEntity = createParameterContextEntity(getTestName(), null, parameters);
        final ParameterContextEntity createdContextEntity = getNifiClient().getParamContextClient().createParamContext(contextEntity);

        // Set the Parameter Context on the root Process Group
        setParameterContext("root", createdContextEntity);

        // Create a Processor and update it to reference Parameter "name"
        ProcessorEntity processorEntity = createProcessor(TEST_PROCESSORS_PACKAGE + ".CountEvents", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());
        final String processorId = processorEntity.getId();

        // Update processor to reference Parameter "name"
        getClientUtil().updateProcessorProperties(processorEntity, Collections.singletonMap("Name", propertyValue));

        waitForValidProcessor(processorId);

        // Create another processor, and start it. We will not reference any Parameters with this one.
        final ProcessorEntity secondProcessorEntity = createProcessor(TEST_PROCESSORS_PACKAGE + ".CountEvents", NIFI_GROUP_ID, TEST_EXTENSIONS_ARTIFACT_ID, getNiFiVersion());

        // Start Processors
        getNifiClient().getProcessorClient().startProcessor(processorEntity.getId(), processorEntity.getRevision().getClientId(), 1L);
        getNifiClient().getProcessorClient().startProcessor(secondProcessorEntity.getId(), secondProcessorEntity.getRevision().getClientId(), 1L);

        Map<String, Long> counterValues = waitForCounter(processorEntity.getId(), "Scheduled", getNumberOfNodes());
        assertFalse(counterValues.containsKey("Stopped"));

        final Set<ParameterEntity> createdParameters = createdContextEntity.getComponent().getParameters();
        createdParameters.clear();
        createdParameters.add(createParameterEntity("name", "Changed Value from bar to baz", false, "baz"));
        final ParameterContextUpdateRequestEntity updateRequestEntity = getNifiClient().getParamContextClient().updateParamContext(createdContextEntity);
        final String requestId = updateRequestEntity.getRequest().getRequestId();

        // Ensure that the Processor is the only Affected Component.
        final Set<AffectedComponentEntity> affectedComponents = updateRequestEntity.getRequest().getReferencingComponents();
        assertEquals(1, affectedComponents.size());
        final AffectedComponentEntity affectedComponentEntity = affectedComponents.iterator().next();
        assertEquals(processorEntity.getId(), affectedComponentEntity.getId());

        // Wait for the update to complete
        getClientUtil().waitForParameterContextRequestToComplete(createdContextEntity.getId(), requestId);
        // Delete the update request
        getNifiClient().getParamContextClient().deleteParamContextUpdateRequest(createdContextEntity.getId(), requestId);

        // Ensure that the Processor is running
        processorEntity = getNifiClient().getProcessorClient().getProcessor(processorId);
        assertEquals("RUNNING", processorEntity.getComponent().getState());

        // Ensure that it has been stopped once and started twice (i.e., it has been restarted). The counters may not immediately
        // reflect that the Processor has been scheduled twice, depending on timing, so loop while waiting for this to happen.
        counterValues = getCountersAsMap(processorEntity.getId());
        assertEquals(getNumberOfNodes(), counterValues.get("Stopped").longValue());

        waitForCounter(processorEntity.getId(), "Scheduled", getNumberOfNodes() * 2);

        // Ensure that the other Processor has been scheduled only once and not stopped.
        counterValues = getCountersAsMap(secondProcessorEntity.getId());
        assertFalse(counterValues.containsKey("Stopped"));
        assertEquals(getNumberOfNodes(), counterValues.get("Scheduled").longValue());
    }


    private Map<String, Long> waitForCounter(final String context, final String counterName, final long expectedValue) throws NiFiClientException, IOException, InterruptedException {
        return getClientUtil().waitForCounter(context, counterName, expectedValue);
    }

    private Map<String, Long> getCountersAsMap(final String processorId) throws NiFiClientException, IOException {
        return getClientUtil().getCountersAsMap(processorId);
    }

    private ProcessorEntity createProcessor(final String type, final String groupId, final String artifactId, final String version) throws NiFiClientException, IOException {
        return getClientUtil().createProcessor(type, groupId, artifactId, version);
    }

    public ControllerServiceEntity createControllerService(final String type, final String processGroupId, final String bundleGroupId, final String artifactId, final String version)
        throws NiFiClientException, IOException {

        return getClientUtil().createControllerService(type, processGroupId, bundleGroupId, artifactId, version);
    }

    public ParameterEntity createParameterEntity(final String name, final String description, final boolean sensitive, final String value) {
        return getClientUtil().createParameterEntity(name, description, sensitive, value);
    }

    public ParameterContextEntity createParameterContextEntity(final String name, final String description, final Set<ParameterEntity> parameters) {
        return getClientUtil().createParameterContextEntity(name, description, parameters);
    }

    private ProcessGroupEntity setParameterContext(final String groupId, final ParameterContextEntity parameterContext) throws NiFiClientException, IOException {
        return getClientUtil().setParameterContext(groupId, parameterContext);
    }

    public ParameterContextEntity createParameterContext(final String parameterName, final String parameterValue) throws NiFiClientException, IOException {
        return createParameterContext(Collections.singletonMap(parameterName, parameterValue));
    }

    public ParameterContextEntity createParameterContext(final Map<String, String> parameters) throws NiFiClientException, IOException {
        return getClientUtil().createParameterContext(getTestName(), parameters);
    }

    public ParameterContextUpdateRequestEntity updateParameterContext(final ParameterContextEntity existingEntity, final String paramName, final String paramValue)
                throws NiFiClientException, IOException {
        return updateParameterContext(existingEntity, Collections.singletonMap(paramName, paramValue));
    }

    public ParameterContextUpdateRequestEntity updateParameterContext(final ParameterContextEntity existingEntity, final Map<String, String> parameters) throws NiFiClientException, IOException {
        return getClientUtil().updateParameterContext(existingEntity, parameters);
    }


    private void waitForValidProcessor(String id) throws InterruptedException, IOException, NiFiClientException {
        getClientUtil().waitForValidProcessor(id);
    }

    private void waitForInvalidProcessor(String id) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().waitForInvalidProcessor(id);
    }

    private void waitForRunningProcessor(final String processorId) throws InterruptedException, IOException, NiFiClientException {
        getClientUtil().waitForRunningProcessor(processorId);
    }

    private void waitForStoppedProcessor(final String processorId) throws InterruptedException, IOException, NiFiClientException {
        getClientUtil().waitForStoppedProcessor(processorId);
    }
}

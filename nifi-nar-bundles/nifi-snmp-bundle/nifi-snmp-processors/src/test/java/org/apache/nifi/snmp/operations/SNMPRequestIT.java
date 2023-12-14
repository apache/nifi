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
package org.apache.nifi.snmp.operations;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPTreeResponse;
import org.apache.nifi.snmp.dto.SNMPValue;
import org.apache.nifi.snmp.exception.RequestTimeoutException;
import org.apache.nifi.snmp.factory.core.SNMPContext;
import org.apache.nifi.snmp.factory.core.SNMPFactoryProvider;
import org.apache.nifi.snmp.helper.configurations.SNMPConfigurationFactory;
import org.apache.nifi.snmp.helper.configurations.SNMPV1V2cConfigurationFactory;
import org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory;
import org.apache.nifi.snmp.testagents.TestAgent;
import org.apache.nifi.snmp.testagents.TestSNMPV1Agent;
import org.apache.nifi.snmp.testagents.TestSNMPV2cAgent;
import org.apache.nifi.snmp.testagents.TestSNMPV3Agent;
import org.apache.nifi.util.EqualsWrapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class SNMPRequestIT {

    private static final String LOCALHOST = "127.0.0.1";
    private static final String INVALID_HOST = "127.0.0.2";
    private static final String READ_ONLY_OID_1 = "1.3.6.1.4.1.32437.1.5.1.4.2.0";
    private static final String READ_ONLY_OID_2 = "1.3.6.1.4.1.32437.1.5.1.4.3.0";
    private static final String WRITE_ONLY_OID = "1.3.6.1.4.1.32437.1.5.1.4.4.0";
    private static final String WALK_OID = "1.3.6.1.4.1.32437";
    private static final String INVALID_OID = "1.3.6.1.4.1.32437.0";
    private static final String READ_ONLY_OID_VALUE_1 = "TestOID1";
    private static final String READ_ONLY_OID_VALUE_2 = "TestOID2";
    private static final String WRITE_ONLY_OID_VALUE = "writeOnlyOID";
    private static final String SNMP_PROP_DELIMITER = "$";
    private static final String SNMP_PROP_PREFIX = "snmp" + SNMP_PROP_DELIMITER;
    private static final String NOT_WRITABLE = "Not writable";
    private static final String NO_ACCESS = "No access";
    private static final String SUCCESS = "Success";
    private static final String NO_SUCH_OBJECT = "noSuchObject";
    private static final String UNABLE_TO_CREATE_OBJECT = "Unable to create object";
    private static final String TEST_OID_VALUE = "testValue";
    private static final String NO_SUCH_NAME = "No such name";
    protected static final Map<String, String> WALK_OID_MAP;

    static {
        final Map<String, String> oidMap = new HashMap<>();
        oidMap.put(READ_ONLY_OID_1, READ_ONLY_OID_VALUE_1);
        oidMap.put(READ_ONLY_OID_2, READ_ONLY_OID_VALUE_2);
        WALK_OID_MAP = Collections.unmodifiableMap(oidMap);
    }

    private static final SNMPConfigurationFactory snmpV1ConfigurationFactory = new SNMPV1V2cConfigurationFactory(SnmpConstants.version1);
    private static final SNMPConfigurationFactory snmpv2cConfigurationFactory = new SNMPV1V2cConfigurationFactory(SnmpConstants.version2c);
    private static final SNMPConfigurationFactory snmpv3ConfigurationFactory = new SNMPV3ConfigurationFactory();

    private static final TestAgent v1TestAgent = new TestSNMPV1Agent(LOCALHOST);
    private static final TestAgent v2cTestAgent = new TestSNMPV2cAgent(LOCALHOST);
    private static final TestAgent v3TestAgent = new TestSNMPV3Agent(LOCALHOST);


    static {
        registerManagedObjects(v1TestAgent);
        registerManagedObjects(v2cTestAgent);
        registerManagedObjects(v3TestAgent);
    }

    private static Stream<Arguments> provideBasicArguments() {
        return Stream.of(
                Arguments.of(SnmpConstants.version1, snmpV1ConfigurationFactory, v1TestAgent),
                Arguments.of(SnmpConstants.version2c, snmpv2cConfigurationFactory, v2cTestAgent),
                Arguments.of(SnmpConstants.version3, snmpv3ConfigurationFactory, v3TestAgent)
        );
    }

    private static Stream<Arguments> provideCannotSetReadOnlyOidArguments() {
        return Stream.of(
                Arguments.of(SnmpConstants.version1, snmpV1ConfigurationFactory, v1TestAgent, NO_SUCH_NAME),
                Arguments.of(SnmpConstants.version2c, snmpv2cConfigurationFactory, v2cTestAgent, NOT_WRITABLE),
                Arguments.of(SnmpConstants.version3, snmpv3ConfigurationFactory, v3TestAgent, NOT_WRITABLE)
        );
    }

    private static Stream<Arguments> provideCannotModifyOidStatusMessageArguments() {
        return Stream.of(
                Arguments.of(SnmpConstants.version1, snmpV1ConfigurationFactory, v1TestAgent, NO_SUCH_NAME),
                Arguments.of(SnmpConstants.version2c, snmpv2cConfigurationFactory, v2cTestAgent, NO_ACCESS),
                Arguments.of(SnmpConstants.version3, snmpv3ConfigurationFactory, v3TestAgent, NO_ACCESS)
        );
    }

    private static Stream<Arguments> provideGetInvalidOidStatusMessageArguments() {
        return Stream.of(
                Arguments.of(SnmpConstants.version1, snmpV1ConfigurationFactory, v1TestAgent, NO_SUCH_NAME),
                Arguments.of(SnmpConstants.version2c, snmpv2cConfigurationFactory, v2cTestAgent, NO_SUCH_OBJECT),
                Arguments.of(SnmpConstants.version3, snmpv3ConfigurationFactory, v3TestAgent, NO_SUCH_OBJECT)
        );
    }

    private static Stream<Arguments> provideSetInvalidOidStatusMessageArguments() {
        return Stream.of(
                Arguments.of(SnmpConstants.version1, snmpV1ConfigurationFactory, v1TestAgent, NO_SUCH_NAME),
                Arguments.of(SnmpConstants.version2c, snmpv2cConfigurationFactory, v2cTestAgent, UNABLE_TO_CREATE_OBJECT),
                Arguments.of(SnmpConstants.version3, snmpv3ConfigurationFactory, v3TestAgent, UNABLE_TO_CREATE_OBJECT)
        );
    }

    @ParameterizedTest
    @MethodSource("provideBasicArguments")
    void testSuccessfulSnmpGet(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent) throws IOException {
        agent.start();
        try {
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpManager);
            final Map<String, String> attributes = Collections.singletonMap("snmp$" + READ_ONLY_OID_1, "");
            final SNMPSingleResponse response = getSNMPHandler.get(attributes, target).orElseThrow();
            assertEquals(READ_ONLY_OID_VALUE_1, response.getVariableBindings().get(0).getVariable());
            assertEquals(SUCCESS, response.getErrorStatusText());
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideBasicArguments")
    void testSuccessfulSnmpGetWithFlowFileInput(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent) throws IOException {
        agent.start();
        try {
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpManager);
            final Optional<SNMPSingleResponse> optionalResponse = getSNMPHandler.get(getFlowFileAttributesForSnmpGet(READ_ONLY_OID_1, READ_ONLY_OID_2), target);
            if (optionalResponse.isPresent()) {
                final SNMPSingleResponse response = optionalResponse.get();
                Set<String> expectedVariables = new HashSet<>(Arrays.asList(READ_ONLY_OID_VALUE_1, READ_ONLY_OID_VALUE_2));
                Set<String> actualVariables = response.getVariableBindings().stream().map(SNMPValue::getVariable).collect(Collectors.toSet());
                assertEquals(expectedVariables, actualVariables);
                assertEquals(SUCCESS, response.getErrorStatusText());
            } else {
                fail("Response is not present.");
            }
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }


    @ParameterizedTest
    @MethodSource("provideBasicArguments")
    void testSuccessfulSnmpWalk(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent) throws IOException {
        agent.start();
        try {
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpManager);
            final Map<String, String> attributes = Collections.singletonMap("snmp$" + WALK_OID, "");
            final Optional<SNMPTreeResponse> response = getSNMPHandler.walk(attributes, target);

            assertSubTreeContainsOids(response.orElseThrow());
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideBasicArguments")
    void testSnmpGetTimeoutReturnsNull(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent) throws IOException {
        agent.start();
        try {
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfigWithCustomHost(INVALID_HOST, agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final Map<String, String> attributes = Collections.singletonMap("snmp$" + READ_ONLY_OID_1, "");
            final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpManager);
            assertThrows(RequestTimeoutException.class, () ->
                    getSNMPHandler.get(attributes, target)
            );
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideBasicArguments")
    void testSnmpGetInvalidOidWithFlowFileInput(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent) throws IOException {
        agent.start();
        try {
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpManager);
            final Optional<SNMPSingleResponse> optionalResponse = getSNMPHandler.get(getFlowFileAttributesForSnmpGet(INVALID_OID, READ_ONLY_OID_2), target);
            if (optionalResponse.isPresent()) {
                final SNMPSingleResponse response = optionalResponse.get();
                final List<SNMPValue> actualVariableBindings = response.getVariableBindings();
                final List<SNMPValue> expectedVariableBindings;

                final List<Function<SNMPValue, Object>> equalsProperties = Arrays.asList(
                        SNMPValue::getOid,
                        SNMPValue::getVariable
                );

                if (version == SnmpConstants.version1) {
                    expectedVariableBindings = Arrays.asList(
                            new SNMPValue(INVALID_OID, "Null"),
                            new SNMPValue(READ_ONLY_OID_2, READ_ONLY_OID_VALUE_2)
                    );
                    assertEquals(NO_SUCH_NAME, response.getErrorStatusText());
                } else {
                    expectedVariableBindings = Arrays.asList(
                            new SNMPValue(INVALID_OID, NO_SUCH_OBJECT),
                            new SNMPValue(READ_ONLY_OID_2, READ_ONLY_OID_VALUE_2)
                    );
                    assertEquals(SUCCESS, response.getErrorStatusText());
                }
                assertEquals(
                        new HashSet<>(EqualsWrapper.wrapList(actualVariableBindings, equalsProperties)),
                        new HashSet<>(EqualsWrapper.wrapList(expectedVariableBindings, equalsProperties))
                );
            } else {
                fail("Response is not present.");
            }
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideBasicArguments")
    void testSuccessfulSnmpSet(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent) throws IOException {
        agent.start();
        try {
            final Map<String, String> flowFileAttributes = getFlowFileAttributes(WRITE_ONLY_OID);
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final SetSNMPHandler setSNMPHandler = new SetSNMPHandler(snmpManager);
            final Optional<SNMPSingleResponse> optionalResponse = setSNMPHandler.set(flowFileAttributes, target);
            if (optionalResponse.isPresent()) {
                final SNMPSingleResponse response = optionalResponse.get();
                assertEquals(TEST_OID_VALUE, response.getVariableBindings().get(0).getVariable());
                assertEquals(SUCCESS, response.getErrorStatusText());
            } else {
                fail("Response is not present.");
            }
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideCannotSetReadOnlyOidArguments")
    void testCannotSetReadOnlyObject(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent,
                                     String cannotSetReadOnlyOidStatusMessage) throws IOException {
        agent.start();
        try {
            final Map<String, String> flowFileAttributes = getFlowFileAttributes(READ_ONLY_OID_1);
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final SetSNMPHandler setSNMPHandler = new SetSNMPHandler(snmpManager);
            final Optional<SNMPSingleResponse> optionalResponse = setSNMPHandler.set(flowFileAttributes, target);
            if (optionalResponse.isPresent()) {
                final SNMPSingleResponse response = optionalResponse.get();
                assertEquals(cannotSetReadOnlyOidStatusMessage, response.getErrorStatusText());
            } else {
                fail("Response is not present.");
            }
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideCannotModifyOidStatusMessageArguments")
    void testCannotGetWriteOnlyObject(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent,
                                      String cannotModifyOidStatusMessage) throws IOException {
        agent.start();
        try {
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpManager);
            final Map<String, String> attributes = Collections.singletonMap("snmp$" + WRITE_ONLY_OID, "");
            final Optional<SNMPSingleResponse> response = getSNMPHandler.get(attributes, target);

            assertEquals(cannotModifyOidStatusMessage, response.map(SNMPSingleResponse::getErrorStatusText).orElseThrow());
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideGetInvalidOidStatusMessageArguments")
    void testCannotGetInvalidOid(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent,
                                 String getInvalidOidStatusMessage) throws IOException {
        agent.start();
        try {
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpManager);
            final Map<String, String> attributes = Collections.singletonMap("snmp$" + INVALID_OID, "");
            final SNMPSingleResponse response = getSNMPHandler.get(attributes, target).orElseThrow();
            if (version == SnmpConstants.version1) {
                assertEquals(getInvalidOidStatusMessage, response.getErrorStatusText());
            } else {
                assertEquals(getInvalidOidStatusMessage, response.getVariableBindings().get(0).getVariable());
                assertEquals(SUCCESS, response.getErrorStatusText());
            }
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideSetInvalidOidStatusMessageArguments")
    void testCannotSetInvalidOid(int version, SNMPConfigurationFactory snmpConfigurationFactory, TestAgent agent,
                                 String setInvalidOidStatusMessage) throws IOException {
        agent.start();
        try {
            final Map<String, String> flowFileAttributes = getFlowFileAttributes(INVALID_OID);
            final SNMPConfiguration snmpConfiguration = snmpConfigurationFactory.createSnmpGetSetConfiguration(agent.getPort());
            final SNMPContext factory = SNMPFactoryProvider.getFactory(version);
            final Target target = factory.createTargetInstance(snmpConfiguration);
            final Snmp snmpManager = factory.createSnmpManagerInstance(snmpConfiguration);
            final SetSNMPHandler setSNMPHandler = new SetSNMPHandler(snmpManager);
            final Optional<SNMPSingleResponse> optionalResponse = setSNMPHandler.set(flowFileAttributes, target);
            if (optionalResponse.isPresent()) {
                final SNMPSingleResponse response = optionalResponse.get();
                assertEquals(setInvalidOidStatusMessage, response.getErrorStatusText());
            } else {
                fail("Response is not present.");
            }
        } catch (Exception e) {
            fail(e);
        } finally {
            agent.stop();
            agent.unregister();
        }
    }

    private Map<String, String> getFlowFileAttributes(String oid) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SNMP_PROP_PREFIX + oid, TEST_OID_VALUE);
        return attributes;
    }

    private Map<String, String> getFlowFileAttributesForSnmpGet(String... oids) {
        final Map<String, String> attributes = new HashMap<>();
        Arrays.stream(oids).forEach(oid -> attributes.put(SNMP_PROP_PREFIX + oid, null));
        return attributes;
    }

    private void assertSubTreeContainsOids(SNMPTreeResponse response) {
        final Map<String, String> attributes = response.getAttributes();
        attributes.entrySet().forEach(this::checkEntryContainsSubString);
    }

    private void checkEntryContainsSubString(Map.Entry<String, String> attribute) {
        final AtomicBoolean isMatch = new AtomicBoolean(false);
        WALK_OID_MAP.forEach((key, value) -> {
            if (!isMatch.get() && attribute.getKey().contains(key)) {
                isMatch.set(true);
                assertEquals(value, attribute.getValue());
            }
        });
        if (!isMatch.get()) {
            fail("Expected OID is not found in subtree.");
        }
    }

    private static void registerManagedObjects(final TestAgent agent) {
        agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(READ_ONLY_OID_1), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(READ_ONLY_OID_VALUE_1)),
                DefaultMOFactory.getInstance().createScalar(new OID(READ_ONLY_OID_2), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(READ_ONLY_OID_VALUE_2)),
                DefaultMOFactory.getInstance().createScalar(new OID(WRITE_ONLY_OID), MOAccessImpl.ACCESS_WRITE_ONLY, new OctetString(WRITE_ONLY_OID_VALUE))
        );
    }
}

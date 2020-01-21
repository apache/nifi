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
package org.apache.nifi.processors.script

import org.apache.nifi.bundle.Bundle
import org.apache.nifi.bundle.BundleCoordinate
import org.apache.nifi.components.validation.ValidationTrigger
import org.apache.nifi.controller.LoggableComponent
import org.apache.nifi.controller.ProcessScheduler
import org.apache.nifi.controller.ProcessorNode
import org.apache.nifi.controller.ReloadComponent
import org.apache.nifi.controller.StandardProcessorNode
import org.apache.nifi.controller.ValidationContextFactory
import org.apache.nifi.controller.service.ControllerServiceProvider
import org.apache.nifi.nar.ExtensionManager
import org.apache.nifi.nar.StandardExtensionDiscoveringManager
import org.apache.nifi.nar.SystemBundle
import org.apache.nifi.parameter.Parameter
import org.apache.nifi.parameter.ParameterDescriptor
import org.apache.nifi.parameter.ParameterReferenceManager
import org.apache.nifi.parameter.StandardParameterContext
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.registry.ComponentVariableRegistry
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.util.MockProcessContext
import org.apache.nifi.util.NiFiProperties
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class InvokeScriptedProcessorTest extends BaseScriptTest {
    private static final Logger logger = LoggerFactory.getLogger(InvokeScriptedProcessorTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
        super.setupInvokeScriptProcessor()

        runner.setValidateExpressionUsage(false)
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy")
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testAddTimeAndThreadAttribute.groovy")
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy")
    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testShouldHandleDynamicSensitivePropertyWithSensitiveParameter() throws Exception {
        // Arrange
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testDynamicSensitiveProperty.groovy")

        // Create parameter context with sensitive parameter
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager()
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager, null)
        ParameterDescriptor testPasswordDescriptor = new ParameterDescriptor.Builder().name("test_password").description("Test Password").sensitive(true).build()
        def parameters = ["test_password": new Parameter(testPasswordDescriptor, "thisIsABadPassword")]
        context.setParameters(parameters)

        InvokeScriptedProcessor isp = runner.processor as InvokeScriptedProcessor
        isp.setup(runner.processContext)

        // Check that the dynamic property descriptor from the script exists

        def pds = isp.propertyDescriptors
        logger.info("Current property descriptors (${pds.size()}): ${pds}")
        def supportedPDs = (isp as InvokeScriptedProcessor).getSupportedPropertyDescriptors()
        logger.info("Supported property descriptors (${supportedPDs.size()}): ${supportedPDs}")
        assert supportedPDs*.name.contains("Password")

        def passwordPD = pds.find { it.name == "Password" }
        assert passwordPD.sensitive
        assert passwordPD.required

        // Assign the sensitive parameter to dynamic sensitive property
        def resolvedPasswordParamPropertyValue = runner.processContext.newPropertyValue("#{test_password}")
        (runner.processContext as MockProcessContext).setProperty(passwordPD, resolvedPasswordParamPropertyValue.value)

        logger.info("Set script body to dynamic sensitive property script")
//        Relationship success = runner.processor.relationships.first()
        runner.assertValid()

        // TODO: Figure out where properties map is originating
        // TODO: Allow getPropertyDescriptor() to check dynamic properties as well (#getSupportedPropertyDescriptors())

        // Act
        runner.run()

        // Assert
//        runner.assertAllFlowFilesTransferred(success, 1)
//        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)
//
//        result.eachWithIndex { MockFlowFile flowFile, int i ->
//            logger.info("Resulting flowfile [${i}] attributes: ${flowFile.attributes}")
//
//            flowFile.assertAttributeExists("time-updated")
//            flowFile.assertAttributeExists("thread")
//            assert flowFile.getAttribute("thread") =~ /pool-\d+-thread-1/
//        }
    }

    /**
     * This test handles a sensitive property added by the script which has already been executed once.
     * @throws Exception
     */
    @Test
    void testGetSupportedPropertiesShouldHandleSensitiveDynamicProperty() throws Exception {
        // Arrange
        def processorProperties = [
                (ScriptingComponentUtils.SCRIPT_FILE.name): TEST_RESOURCE_LOCATION + "groovy/testDynamicSensitiveProperty.groovy",
                (ScriptingComponentUtils.SCRIPT_BODY.name): "",
                (ScriptingComponentUtils.MODULES.name)    : "",
                "Script Engine"                           : "groovy",
                "Password"                                : "#{test_password}"
        ]

        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testDynamicSensitiveProperty.groovy")

        // Create parameter context with sensitive parameter
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager()
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager, null)
        ParameterDescriptor testPasswordDescriptor = new ParameterDescriptor.Builder().name("test_password").description("Test Password").sensitive(true).build()
        def parameters = ["test_password": new Parameter(testPasswordDescriptor, "thisIsABadPassword")]
        context.setParameters(parameters)

        // Run the script once to add the dynamic sensitive property
        InvokeScriptedProcessor isp = runner.processor as InvokeScriptedProcessor
        isp.setup(runner.processContext)

        // Check that the dynamic property descriptor from the script exists
        def pds = isp.propertyDescriptors
        logger.info("Current property descriptors (${pds.size()}): ${pds}")
        def supportedPDs = (isp as InvokeScriptedProcessor).getSupportedPropertyDescriptors()
        logger.info("Supported property descriptors (${supportedPDs.size()}): ${supportedPDs}")
        assert supportedPDs*.name.contains("Password")

        def passwordPD = pds.find { it.name == "Password" }
        assert passwordPD.sensitive
        assert passwordPD.required

        // Assign the sensitive parameter to dynamic sensitive property
        def resolvedPasswordParamPropertyValue = runner.processContext.newPropertyValue("#{test_password}")
        (runner.processContext as MockProcessContext).setProperty(passwordPD, resolvedPasswordParamPropertyValue.value)

        logger.info("Set script body to dynamic sensitive property script")
        runner.assertValid()

        ProcessorNode processorNode = mockProcessorNodeCollaborators(isp, "isp-processor-get-supported-properties-dynamic-sensitive")

        // Act

        // This calls AbstractComponentNode.updateProperties and verifyCanUpdateProperties, which is where the mismatch occurs
        processorNode.setProperties(processorProperties)
        logger.info("Successfully set processor ${processorNode} properties to ${processorProperties}")

        // Assert
        def passwordPDFromNode = processorNode.getPropertyDescriptor("Password")
        logger.info("Retrieved password PD is sensitive: ${passwordPDFromNode.sensitive}")
        assert passwordPDFromNode.sensitive
    }

    /**
     * This test handles a sensitive property (added by the script) which has not been executed during application startup.
     * @throws Exception
     */
    @Test
    void testGetSupportedPropertiesShouldHandleSensitiveDynamicPropertyDuringNiFiStart() throws Exception {
        // Arrange
        def processorProperties = [
                (ScriptingComponentUtils.SCRIPT_FILE.name): TEST_RESOURCE_LOCATION + "groovy/testDynamicSensitiveProperty.groovy",
                (ScriptingComponentUtils.SCRIPT_BODY.name): "",
                (ScriptingComponentUtils.MODULES.name)    : "",
                "Script Engine"                           : "groovy",
                "Password"                                : "#{test_password}"
        ]

        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testDynamicSensitiveProperty.groovy")

        // Create parameter context with sensitive parameter
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager()
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager, null)
        ParameterDescriptor testPasswordDescriptor = new ParameterDescriptor.Builder().name("test_password").description("Test Password").sensitive(true).build()
        def parameters = ["test_password": new Parameter(testPasswordDescriptor, "thisIsABadPassword")]
        context.setParameters(parameters)

        // Run the script once to add the dynamic sensitive property
        InvokeScriptedProcessor isp = runner.processor as InvokeScriptedProcessor
//        isp.setup(runner.processContext)

        // Check that the dynamic property descriptor from the script does not exist
        def pds = isp.propertyDescriptors
        logger.info("Current property descriptors (${pds.size()}): ${pds}")
        def supportedPDs = (isp as InvokeScriptedProcessor).getSupportedPropertyDescriptors()
        logger.info("Supported property descriptors (${supportedPDs.size()}): ${supportedPDs}")
        assert !supportedPDs*.name.contains("Password")

//        def passwordPD = pds.find { it.name == "Password" }
//        assert passwordPD.sensitive
//        assert passwordPD.required

        // Assign the sensitive parameter to dynamic sensitive property
//        def resolvedPasswordParamPropertyValue = runner.processContext.newPropertyValue("#{test_password}")
//        (runner.processContext as MockProcessContext).setProperty(passwordPD, resolvedPasswordParamPropertyValue.value)

        logger.info("Set script body to dynamic sensitive property script")
        runner.assertValid()

        ProcessorNode processorNode = mockProcessorNodeCollaborators(isp, "isp-processor-get-supported-properties-dynamic-sensitive-first-run")

        // Act

        /* This calls AbstractComponentNode.updateProperties and verifyCanUpdateProperties, which is where the mismatch occurs
         The properties saved in the flow.xml.gz contain the Password -> "#{test_password}" definition but the processor doesn't have the "added" property descriptor defined yet
         This assumes any not-present PD is a dynamic property and dynamic properties cannot be sensitive
         We can't just allow dynamic properties to be sensitive because then any user could add a dynamic property and expose sensitive parameters
         Perhaps an exception should be made for ISP and ES/EGS because they can modify PDs anyway?
         */
        processorNode.setProperties(processorProperties)
        logger.info("Successfully set processor ${processorNode} properties to ${processorProperties}")

        // Assert
        def passwordPDFromNode = processorNode.getPropertyDescriptor("Password")
        logger.info("Retrieved password PD is sensitive: ${passwordPDFromNode.sensitive}")
        assert passwordPDFromNode.sensitive
    }

    private static ProcessorNode mockProcessorNodeCollaborators(InvokeScriptedProcessor isp, String uuid = "isp-processor-mocked-collaborators") {
        ValidationContextFactory mockValidationContextFactory = [:] as ValidationContextFactory
        ProcessScheduler mockProcessScheduler = [:] as ProcessScheduler
        ControllerServiceProvider mockControllerServiceProvider = [:] as ControllerServiceProvider
        ComponentVariableRegistry mockVariableRegistry = [:] as ComponentVariableRegistry
        ReloadComponent mockReloadComponent = [:] as ReloadComponent
        ExtensionManager extensionManager = buildExtensionManager()
        ValidationTrigger mockValidationTrigger = [triggerAsync: { ProcessorNode pn -> }] as ValidationTrigger
        BundleCoordinate mockBundleCoordinate = new BundleCoordinate("org.apache.nifi.processors.script", "mock-bundle-coordinate", "1.0.0")
        LoggableComponent logWrapper = new LoggableComponent(isp, mockBundleCoordinate, null)

        // Create StandardProcessorNode wrapper around ISP processor
        ProcessorNode processorNode = new StandardProcessorNode(logWrapper, uuid, mockValidationContextFactory, mockProcessScheduler, mockControllerServiceProvider, mockVariableRegistry, mockReloadComponent, extensionManager, mockValidationTrigger)
//        processorNode.pauseValidationTrigger()
        processorNode
    }

    private static ExtensionManager buildExtensionManager() {
        NiFiProperties niFiProperties = new StandardNiFiProperties(new Properties([
                (NiFiProperties.NAR_LIBRARY_DIRECTORY): "target/lib"
        ]))
        Bundle systemBundle = SystemBundle.create(niFiProperties)

//        BundleDetails mockBundleDetails = new BundleDetails.Builder()
//                .workingDir(new File("target/"))
//                .coordinate(new BundleCoordinate("org.apache.nifi.processors.script",
//                "test-system",
//                "1.0.0"))
//                .build()
//        Bundle mockSystemBundle = new Bundle(mockBundleDetails, ClassLoader.getSystemClassLoader())
        def extensionManager = new StandardExtensionDiscoveringManager()
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet())
        extensionManager
    }
}

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
package org.apache.nifi.controller


import org.apache.nifi.bundle.Bundle
import org.apache.nifi.bundle.BundleCoordinate
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.validation.ValidationTrigger
import org.apache.nifi.controller.service.ControllerServiceProvider
import org.apache.nifi.groups.ProcessGroup
import org.apache.nifi.lookup.script.BaseScriptedLookupService
import org.apache.nifi.lookup.script.ScriptedLookupService
import org.apache.nifi.nar.ExtensionManager
import org.apache.nifi.nar.StandardExtensionDiscoveringManager
import org.apache.nifi.nar.SystemBundle
import org.apache.nifi.parameter.Parameter
import org.apache.nifi.parameter.ParameterContext
import org.apache.nifi.parameter.ParameterDescriptor
import org.apache.nifi.parameter.ParameterReferenceManager
import org.apache.nifi.parameter.StandardParameterContext
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Processor
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript
import org.apache.nifi.processors.script.BaseScriptTest
import org.apache.nifi.processors.script.ExecuteScript
import org.apache.nifi.processors.script.HashMapParameterReferenceManager
import org.apache.nifi.processors.script.InvokeScriptedProcessor
import org.apache.nifi.record.script.AbstractScriptedRecordFactory
import org.apache.nifi.record.script.ScriptedReader
import org.apache.nifi.record.script.ScriptedRecordSetWriter
import org.apache.nifi.record.sink.script.ScriptedRecordSink
import org.apache.nifi.registry.ComponentVariableRegistry
import org.apache.nifi.reporting.script.ScriptedReportingTask
import org.apache.nifi.rules.engine.script.ScriptedRulesEngine
import org.apache.nifi.rules.handlers.script.ScriptedActionHandler
import org.apache.nifi.script.AbstractScriptedControllerService
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This test exercises functionality in {@link AbstractComponentNode} but does so with an
 * explicit use of the {@link org.apache.nifi.processors.script.InvokeScriptedProcessor} as
 * the underlying processor to allow usage of the dynamic property descriptors.
 */
@RunWith(JUnit4.class)
class ISPAwareAbstractComponentNodeTest extends BaseScriptTest {
    private static final Logger logger = LoggerFactory.getLogger(ISPAwareAbstractComponentNodeTest.class)

    private AbstractComponentNode abstractComponentNode

    private Map<String, String> DEFAULT_PROPERTIES = [
            "Script File"     : "target/some/path",
            "Script Body"     : "",
            "Module Directory": "",
            "Script Engine"   : "groovy"
    ]

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
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testDynamicSensitiveProperty.groovy")
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy")
    }

    @After
    void tearDown() throws Exception {

    }

    private static ExtensionManager buildExtensionManager() {
        NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null,
                [(NiFiProperties.NAR_LIBRARY_DIRECTORY): "target/lib"])
        Bundle systemBundle = SystemBundle.create(niFiProperties)

        def extensionManager = new StandardExtensionDiscoveringManager()
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet())
        extensionManager
    }

    private static ProcessorNode mockProcessorNodeCollaborators(Processor processor, String uuid = "isp-processor-mocked-collaborators") {
        ValidationContextFactory mockValidationContextFactory = [:] as ValidationContextFactory
        ProcessScheduler mockProcessScheduler = [:] as ProcessScheduler
        ControllerServiceProvider mockControllerServiceProvider = [:] as ControllerServiceProvider
        ComponentVariableRegistry mockVariableRegistry = [:] as ComponentVariableRegistry
        ReloadComponent mockReloadComponent = [:] as ReloadComponent
        ExtensionManager extensionManager = buildExtensionManager()
        ValidationTrigger mockValidationTrigger = [triggerAsync: { ProcessorNode pn -> }] as ValidationTrigger
        BundleCoordinate mockBundleCoordinate = new BundleCoordinate("org.apache.nifi.processors.script", "mock-bundle-coordinate", "1.0.0")
        LoggableComponent logWrapper = new LoggableComponent(processor, mockBundleCoordinate, null)

        // Create StandardProcessorNode wrapper around processor
        String componentType = processor.class.simpleName
        String canonicalClassName = processor.class.canonicalName

        // Override the component type and class name for ISP because the actual instance is AccessibleInvokeScriptedProcessor
        if (processor.class.name =~ /(?i)script/) {
            componentType = "InvokeScriptedProcessor"
            canonicalClassName = "org.apache.nifi.processors.script.InvokeScriptedProcessor"
        }

        // Use the explicit constructor for the component type and canonical class because BaseScriptTest.AccessibleInvokeScriptedProcessor is not in the explicit list
        ProcessorNode processorNode = new StandardProcessorNode(logWrapper, uuid, mockValidationContextFactory, mockProcessScheduler, mockControllerServiceProvider, componentType, canonicalClassName, mockVariableRegistry, mockReloadComponent, extensionManager, mockValidationTrigger, false)
        processorNode
    }

    @Test
    void testShouldDetectScriptableComponents() {
        // Arrange
        def isp = TestRunners.newTestRunner(InvokeScriptedProcessor.class).processor
        def es = TestRunners.newTestRunner(ExecuteScript.class).processor
        def egs = TestRunners.newTestRunner(ExecuteGroovyScript.class).processor
        def sr = [getIdentifier: { -> "id" }] as ScriptedReader
        def srsw = [getIdentifier: { -> "id" }] as ScriptedRecordSetWriter
        def sls = [getIdentifier: { -> "id" }] as  ScriptedLookupService
        def srs = [getIdentifier: { -> "id" }] as  ScriptedRecordSink
        def srt = [getIdentifier: { -> "id" }] as  ScriptedReportingTask
        def sre = [getIdentifier: { -> "id" }] as  ScriptedRulesEngine
        def sah = [getIdentifier: { -> "id" }] as  ScriptedActionHandler
        def ascs = [setup: { -> }, reloadScript: { String scriptBody -> false }, getIdentifier: { -> "id" }] as AbstractScriptedControllerService
        def asrf = [reloadScript: { String scriptBody -> false }, getIdentifier: { -> "id" }] as AbstractScriptedRecordFactory
        def bsls = [getIdentifier: { -> "id" }] as BaseScriptedLookupService
        def scriptableComponents = [isp, es, egs, sr, srsw, sls, srs, srt, sre, sah, ascs, asrf, bsls]

        def concreteProcessor = TestRunners.newTestRunner(new SimpleProcessor()).processor
        def nonScriptableComponents = [concreteProcessor, null]

        // Act
        def scriptableComponentResults = scriptableComponents.collectEntries {
            [it, AbstractComponentNode.isScriptableComponent(it)]
        }

        def nonScriptableComponentResults = nonScriptableComponents.collectEntries {
            [it, AbstractComponentNode.isScriptableComponent(it)]
        }

        // Assert
        assert scriptableComponentResults.values().every()
        assert nonScriptableComponentResults.values().every { !it }
    }

    @Test
    void testScriptableComponentShouldHandleDynamicSensitivePropertyWithSensitiveParameter() throws Exception {
        // Arrange
        ParameterContext parameterContext = createParameterContext()

        InvokeScriptedProcessor isp = runner.processor as InvokeScriptedProcessor

        // Check that the dynamic property descriptor from the script does not exist
        def pds = isp.propertyDescriptors
        logger.info("Current property descriptors (${pds.size()}): ${pds}")
        def supportedPDs = (isp as InvokeScriptedProcessor).getSupportedPropertyDescriptors()
        logger.info("Supported property descriptors (${supportedPDs.size()}): ${supportedPDs}")
        assert !supportedPDs*.name.contains("Password")

        // Set up ACN with relevant values
        abstractComponentNode = mockProcessorNodeCollaborators(isp)
        ProcessGroup pg = [getParameterContext: { -> parameterContext }] as ProcessGroup
        abstractComponentNode.setProcessGroup(pg)

        // Set up properties map
        def properties = DEFAULT_PROPERTIES << ["Password": "#{test_password}"]
        logger.info("Properties (${properties.size()}): ${properties}")

        // Act
        abstractComponentNode.setProperties(properties)

        // Assert for a scriptable component, the PD is set accordingly
        def retrievedPDs = isp.propertyDescriptors
        assert retrievedPDs.size() == 4
        assert !retrievedPDs*.name.contains("Password")

        // Run the #setup() method which would be triggered on @OnScheduled
        isp.setup(runner.processContext)

        // Assert

        // Assert for a scriptable component, the PD is set accordingly
        def postSetupPDs = isp.getSupportedPropertyDescriptors()
        assert postSetupPDs.size() == 5
        assert isp.getPropertyDescriptor("Password").sensitive
    }

    @Test
    void testScriptableComponentShouldHandleDynamicSensitivePropertyWithLiteralValue() throws Exception {
        // Arrange
        InvokeScriptedProcessor isp = runner.processor as InvokeScriptedProcessor

        // Check that the dynamic property descriptor from the script does not exist
        def pds = isp.propertyDescriptors
        logger.info("Current property descriptors (${pds.size()}): ${pds}")
        def supportedPDs = (isp as InvokeScriptedProcessor).getSupportedPropertyDescriptors()
        logger.info("Supported property descriptors (${supportedPDs.size()}): ${supportedPDs}")
        assert !supportedPDs*.name.contains("Password")

        // Set up ACN with relevant values
        abstractComponentNode = mockProcessorNodeCollaborators(isp)

        // Set up properties map
        def properties = DEFAULT_PROPERTIES << ["Password": "literal_test_password"]
        logger.info("Properties (${properties.size()}): ${properties}")

        // Act
        abstractComponentNode.setProperties(properties)

        // Assert for a scriptable component, the PD is set accordingly
        def retrievedPDs = isp.propertyDescriptors
        assert retrievedPDs.size() == 4
        assert !retrievedPDs*.name.contains("Password")

        // Run the #setup() method which would be triggered on @OnScheduled
        isp.setup(runner.processContext)

        // Assert

        // Assert for a scriptable component, the PD is set accordingly
        def postSetupPDs = isp.getSupportedPropertyDescriptors()
        assert postSetupPDs.size() == 5
        assert isp.getPropertyDescriptor("Password").sensitive
    }

    @Test
    void testRegularComponentShouldHandleDynamicSensitivePropertyWithSensitiveParameter() throws Exception {
        // Arrange
        ParameterContext parameterContext = createParameterContext()

        runner = TestRunners.newTestRunner(new SimpleProcessor())
        SimpleProcessor sp = runner.processor as SimpleProcessor

        // Check that only the expected property descriptors exist
        def pds = sp.propertyDescriptors
        logger.info("Current property descriptors (${pds.size()}): ${pds}")
        def supportedPDs = sp.getSupportedPropertyDescriptors()
        logger.info("Supported property descriptors (${supportedPDs.size()}): ${supportedPDs}")
        assert !supportedPDs*.name.contains("Password")

        // Set up ACN with relevant values
        abstractComponentNode = mockProcessorNodeCollaborators(sp, "simple-processor-node")

        // Set up properties map
        def properties = ["color": "red", "size": "large", "Password": "#{test_password}"]
        logger.info("Properties (${properties.size()}): ${properties}")

        // Act
        abstractComponentNode.setProperties(properties)

        // Assert

        // Assert for a regular component, the PD is not set
        def retrievedPDs = sp.propertyDescriptors
        assert retrievedPDs.size() == 2
        assert !retrievedPDs*.name.contains("Password")
    }

    /**
     * Returns a {@link ParameterContext} populated with at least one sensitive parameter. Optional provided parameters are also populated.
     *
     * @param optionalParameters
     */
    private static ParameterContext createParameterContext(Map<String, String> optionalParameters = [:]) {
        // Create parameter context with sensitive parameter
        def parameterInputs = ["test_password": "thisIsABadPassword"] << optionalParameters

        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager()
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager, null)
        Map<String, Parameter> parameters = parameterInputs.collectEntries { String paramName, String paramValue ->
            ParameterDescriptor paramDescriptor = new ParameterDescriptor.Builder().name(paramName).description(paramName).sensitive(true).build()
            [paramName, new Parameter(paramDescriptor, paramValue)]
        }
        context.setParameters(parameters)
        context
    }

    @Test
    void testRegularComponentShouldHandleDynamicSensitivePropertyWithLiteralValue() throws Exception {
        // Arrange
        runner = TestRunners.newTestRunner(new SimpleProcessor())
        SimpleProcessor sp = runner.processor as SimpleProcessor

        // Check that only the expected property descriptors exist
        def pds = sp.propertyDescriptors
        logger.info("Current property descriptors (${pds.size()}): ${pds}")
        def supportedPDs = sp.getSupportedPropertyDescriptors()
        logger.info("Supported property descriptors (${supportedPDs.size()}): ${supportedPDs}")
        assert !supportedPDs*.name.contains("Password")

        // Set up ACN with relevant values
        abstractComponentNode = mockProcessorNodeCollaborators(sp, "simple-processor-node")

        // Set up properties map
        def properties = ["color": "red", "size": "large", "Password": "literal_test_password"]
        logger.info("Properties (${properties.size()}): ${properties}")

        // Act
        abstractComponentNode.setProperties(properties)

        // Assert

        // Assert for a regular component, the PD is not set
        def retrievedPDs = sp.propertyDescriptors
        assert retrievedPDs.size() == 2
        assert !retrievedPDs*.name.contains("Password")
    }


    /**
     * Generic concrete processor implementation used in tests. This component is not scriptable.
     */
    private class SimpleProcessor extends AbstractProcessor {
        private static final Logger logger = LoggerFactory.getLogger(SimpleProcessor.class)

        private PropertyDescriptor colorPD = new PropertyDescriptor.Builder()
                .name("color")
                .displayName("Color")
                .sensitive(false)
                .required(true)
                .defaultValue("blue")
                .allowableValues("blue", "green", "red")
                .build()

        private PropertyDescriptor sizePD = new PropertyDescriptor.Builder()
                .name("size")
                .displayName("Size")
                .sensitive(false)
                .required(false)
                .defaultValue("medium")
                .allowableValues("small", "medium", "large")
                .build()

        private List descriptors

        @Override
        protected void init(final ProcessorInitializationContext context) {
            def pds = [colorPD, sizePD]
            this.descriptors = Collections.unmodifiableList(pds)
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            descriptors
        }

        @Override
        void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            logger.info("Ran SimpleProcessor#onTrigger with color ${context.getProperty("color")} and size ${context.getProperty("size")}")
        }
    }
}

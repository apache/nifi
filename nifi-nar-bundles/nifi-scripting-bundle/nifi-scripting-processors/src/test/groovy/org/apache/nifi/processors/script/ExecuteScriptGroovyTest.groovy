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


import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.StopWatch
import org.apache.nifi.util.TestRunners
import org.junit.*
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

import static org.junit.Assert.*

@RunWith(JUnit4.class)
class ExecuteScriptGroovyTest extends BaseScriptTest {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteScriptGroovyTest.class)

    @BeforeClass
    public static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setupExecuteScript()

        runner.setValidateExpressionUsage(false)
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy")
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testAddTimeAndThreadAttribute.groovy")
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy")
    }

    @After
    public void tearDown() throws Exception {

    }

    private void setupPooledExecuteScript(int poolSize = 2) {
        final ExecuteScript executeScript = new ExecuteScript()
        // Need to do something to initialize the properties, like retrieve the list of properties
        assertNotNull(executeScript.getSupportedPropertyDescriptors())
        runner = TestRunners.newTestRunner(executeScript)
        runner.setValidateExpressionUsage(false)
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy")
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testAddTimeAndThreadAttribute.groovy")
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy")

        // Override userContext value
        runner.processContext.maxConcurrentTasks = poolSize
        logger.info("Overrode userContext max concurrent tasks to ${runner.processContext.maxConcurrentTasks}")
    }

    @Test
    public void testShouldExecuteScript() throws Exception {
        // Arrange
        final String SINGLE_POOL_THREAD_PATTERN = /pool-\d+-thread-1/

        logger.info("Mock flowfile queue contents: ${runner.queueSize} ${runner.flowFileQueue.queue}")
        runner.assertValid()

        // Act
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1)
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)
        MockFlowFile flowFile = result.get(0)
        logger.info("Resulting flowfile attributes: ${flowFile.attributes}")

        flowFile.assertAttributeExists("time-updated")
        flowFile.assertAttributeExists("thread")
        assert flowFile.getAttribute("thread") =~ SINGLE_POOL_THREAD_PATTERN
    }

    @Test
    public void testShouldSeeDynamicRelationships() throws Exception {
        logger.info("Mock flowfile queue contents: ${runner.queueSize} ${runner.flowFileQueue.queue}")
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "true")
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testDynamicRelationships.groovy")
        runner.setProperty("REL_test", "a test relationship")
        runner.assertValid()

        // Act
        runner.run()

        // Assert
        [ExecuteScript.REL_SUCCESS, "test"].each {
            runner.assertTransferCount(it, 1)
            final List<MockFlowFile> result = runner.getFlowFilesForRelationship(it)
            MockFlowFile flowFile = result.get(0)
            logger.info("Resulting flowfile attributes: ${flowFile.attributes}")
            flowFile.assertAttributeExists("time-updated")
        }
    }

    @Test
    public void testPermissivenessOfRelationshipNaming() throws Exception {
        logger.info("Mock flowfile queue contents: ${runner.queueSize} ${runner.flowFileQueue.queue}")
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testDynamicRelationships.groovy")
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "true")
        ["test", "123", "", "hello, world!", " non-äscii works äs wöll"].each { relName ->
            def propName = "REL_${relName}"
            def description = "dynamic relationship '${relName}'"
            runner.setProperty(propName, description)
            runner.assertValid()
            assertTrue("relationship '${relName}' should exist", runner.processor.relationships.any { it.name == relName })
            assertTrue("relationship '${relName}' should have proper description",
                    runner.processor.relationships.any { it.description == description })
            runner.removeProperty(propName)
            runner.assertValid()
            assertFalse("relationship '${relName}' should not exist", runner.processor.relationships.any { it.name == relName })
        }
    }

    @Test
    public void testDynamicRelationshipsBeingOfByDefault() {
        logger.info("Mock flowfile queue contents: ${runner.queueSize} ${runner.flowFileQueue.queue}")
        def key = "REL_not_a_relationship"
        def value = "not a description"
        runner.setProperty(key, value)
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testNoDynamicRelationships.groovy")
        runner.assertValid()
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "true")
        runner.assertValid()
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "false")

        assertEquals("only 2 relationships exist", 2, runner.processor.relationships.size())
        assertTrue("success relationship exists", runner.processor.relationships.contains(ExecuteScript.REL_SUCCESS))
        assertTrue("failure relationship exists", runner.processor.relationships.contains(ExecuteScript.REL_FAILURE))
        runner.run()
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS)
        runner.assertTransferCount(ExecuteScript.REL_SUCCESS, 1)
        def flowFile = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)[0]
        flowFile.assertAttributeExists("attribute1")
        flowFile.assertAttributeEquals("attribute1", value)
    }

    @Test
    public void testDynamicRelationshipsShadowDynamicProperties() {
        def key = "REL_not_a_relationship"
        def value = "not a description"
        runner.setProperty(key, value)
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "true")
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testNoDynamicRelationships.groovy")

        assertEquals("exactly 3 relationships exist", 3, runner.processor.relationships.size())
        def exception = null
        try {
            runner.run()
        } catch (AssertionError e) {
            exception = e
        }
        assertNotNull("AssertionError was thrown", exception)
        def cause = exception
        while (cause != null) {
            exception = cause
            cause = exception.cause
        }
        assertEquals("cause is MissingPropertyException", MissingPropertyException.class, exception.class)
        assertEquals("missing property is '${key}'", key, exception.property)
    }

    @Test
    public void testReenablingOfDynamicRelationships() {
        def key = "REL_my_relationship"
        def relName = "my_relationship"
        def value = "my description"
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testNoDynamicRelationships.groovy")
        runner.assertValid()
        assertEquals("2 relationships are present", 2, runner.processor.relationships.size())
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "false")
        runner.setProperty(key, value)
        runner.assertValid()
        assertEquals("2 relationships are present", 2, runner.processor.relationships.size())
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "true")
        runner.assertValid()
        assertEquals("3 relationships are present", 3, runner.processor.relationships.size())
        assertTrue("success relationships is present", runner.processor.relationships.contains(ExecuteScript.REL_SUCCESS))
        assertTrue("failure relationships is present", runner.processor.relationships.contains(ExecuteScript.REL_FAILURE))
        def dynamicRelationship = runner.processor.relationships.find {
            it != ExecuteScript.REL_SUCCESS && it != ExecuteScript.REL_FAILURE
        }
        assertEquals("dynamic relationship is has correct name", relName, dynamicRelationship.name)
        assertEquals("dynamic relationship is has correct description", value, dynamicRelationship.description)
        runner.setProperty(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS, "false")
        runner.assertValid()
        assertEquals("2 relationships are present", 2, runner.processor.relationships.size())
    }

    @Test
    public void testShouldExecuteScriptSerially() throws Exception {
        // Arrange
        final int ITERATIONS = 10

        logger.info("Mock flowfile queue contents: ${runner.queueSize} ${runner.flowFileQueue.queue}")
        runner.assertValid()

        // Act
        runner.run(ITERATIONS)

        // Assert
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, ITERATIONS)
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)

        result.eachWithIndex { MockFlowFile flowFile, int i ->
            logger.info("Resulting flowfile [${i}] attributes: ${flowFile.attributes}")

            flowFile.assertAttributeExists("time-updated")
            flowFile.assertAttributeExists("thread")
            assert flowFile.getAttribute("thread") =~ /pool-\d+-thread-1/
        }
    }

    @Test
    public void testShouldExecuteScriptWithPool() throws Exception {
        // Arrange
        final int ITERATIONS = 10
        final int POOL_SIZE = 2

        setupPooledExecuteScript(POOL_SIZE)
        logger.info("Set up ExecuteScript processor with pool size: ${POOL_SIZE}")

        runner.setThreadCount(POOL_SIZE)

        logger.info("Mock flowfile queue contents: ${runner.queueSize} ${runner.flowFileQueue.queue}")
        runner.assertValid()

        // Act
        runner.run(ITERATIONS)

        // Assert
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, ITERATIONS)
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)

        result.eachWithIndex { MockFlowFile flowFile, int i ->
            logger.info("Resulting flowfile [${i}] attributes: ${flowFile.attributes}")

            flowFile.assertAttributeExists("time-updated")
            flowFile.assertAttributeExists("thread")
            assert flowFile.getAttribute("thread") =~ /pool-\d+-thread-[1-${POOL_SIZE}]/
        }
    }

    @Ignore("This test fails intermittently when the serial execution happens faster than pooled")
    @Test
    public void testPooledExecutionShouldBeFaster() throws Exception {
        // Arrange
        final int ITERATIONS = 1000
        final int POOL_SIZE = 4

        // Act
        // Run serially and capture the timing
        final StopWatch stopWatch = new StopWatch(true)
        runner.run(ITERATIONS)
        stopWatch.stop()
        final long serialExecutionTime = stopWatch.getDuration(TimeUnit.MILLISECONDS)
        logger.info("Serial execution time for ${ITERATIONS} executions: ${serialExecutionTime} ms")

        // Assert (1)
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, ITERATIONS)
        final List<MockFlowFile> serialResults = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)

        // Now run parallel
        setupPooledExecuteScript(POOL_SIZE)
        logger.info("Set up ExecuteScript processor with pool size: ${POOL_SIZE}")
        runner.setThreadCount(POOL_SIZE)
        runner.assertValid()

        stopWatch.start()
        runner.run(ITERATIONS)
        stopWatch.stop()
        final long parallelExecutionTime = stopWatch.getDuration(TimeUnit.MILLISECONDS)
        logger.info("Parallel execution time for ${ITERATIONS} executions using ${POOL_SIZE} threads: ${parallelExecutionTime} ms")

        // Assert (2)
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, ITERATIONS)
        final List<MockFlowFile> parallelResults = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)

        parallelResults.eachWithIndex { MockFlowFile flowFile, int i ->
            flowFile.assertAttributeExists("time-updated")
            flowFile.assertAttributeExists("thread")
            assert flowFile.getAttribute("thread") =~ /pool-\d+-thread-[1-${POOL_SIZE}]/
        }

        serialResults.eachWithIndex { MockFlowFile flowFile, int i ->
            flowFile.assertAttributeExists("time-updated")
            flowFile.assertAttributeExists("thread")
            assert flowFile.getAttribute("thread") =~ /pool-\d+-thread-1/
        }

        assert serialExecutionTime > parallelExecutionTime
    }
}

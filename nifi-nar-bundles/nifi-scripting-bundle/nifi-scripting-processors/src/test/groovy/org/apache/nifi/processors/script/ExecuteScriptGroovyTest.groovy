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
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

import static org.junit.Assert.assertNotNull

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

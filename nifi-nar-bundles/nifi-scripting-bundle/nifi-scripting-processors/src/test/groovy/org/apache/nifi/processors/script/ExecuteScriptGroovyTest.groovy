package org.apache.nifi.processors.script

import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.Assert.assertNotNull

@RunWith(JUnit4.class)
class ExecuteScriptGroovyTest extends BaseScriptTest {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteScriptGroovyTest.class)

    private static final String TEST_CSV_DATA = "gender,title,first,last\n" +
            "female,miss,marlene,shaw\n" +
            "male,mr,todd,graham"

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
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy")
        runner.setProperty(ExecuteScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testAddTimeAndThreadAttribute.groovy")
        runner.setProperty(ExecuteScript.MODULES, TEST_RESOURCE_LOCATION + "groovy")
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
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy")
        runner.setProperty(ExecuteScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testAddTimeAndThreadAttribute.groovy")
        runner.setProperty(ExecuteScript.MODULES, TEST_RESOURCE_LOCATION + "groovy")

        // Override context value
        runner.processContext.maxConcurrentTasks = poolSize
        logger.info("Overrode context max concurrent tasks to ${runner.processContext.maxConcurrentTasks}")

        // Must set context properties on runner before calling setup with pool size
//        executeScript.setup(poolSize)
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
        ITERATIONS.times { int i ->
            logger.info("Running iteration ${i}")
            runner.run()
        }

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
        ITERATIONS.times { int i ->
            logger.info("Running iteration ${i}")
            runner.run()
        }

        // Assert
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, ITERATIONS)
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS)

        result.eachWithIndex { MockFlowFile flowFile, int i ->
            logger.info("Resulting flowfile [${i}] attributes: ${flowFile.attributes}")

            flowFile.assertAttributeExists("time-updated")
            flowFile.assertAttributeExists("thread")
            flowFile.assertAttributeEquals("thread", "pool-${i + 1}-thread-1")
        }
    }

    //testShouldExecuteScriptWithPool
    //testShouldHandleFailingScript
    //testShouldHandleNoAvailableEngine
    //testPooledExecutionShouldBeFaster
    //testPoolSizeVsThreadCount
}

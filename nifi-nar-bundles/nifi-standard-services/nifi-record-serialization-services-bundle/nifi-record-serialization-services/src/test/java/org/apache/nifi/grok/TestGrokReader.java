package org.apache.nifi.grok;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class TestGrokReader {

    private String originalHome = "";

    @Before
    public void beforeEach() {
        originalHome = System.getProperty("user.home");
    }

    @After
    public void afterEach() {
        System.setProperty("user.home", originalHome);
    }

    @Test
    public void testSetupOfGrokReaderService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestRegistryProcessor.class);
        final GrokReader reader = new GrokReader();

        runner.addControllerService("grok-schema-registry-service", reader);

        runner.setProperty(reader, GrokReader.PATTERN_FILE, "src/test/resources/grok/grok-pattern-file.txt");
        runner.setProperty(reader, GrokReader.GROK_EXPRESSION, "%{GREEDYDATA:message}");
        runner.setProperty(reader, GrokReader.NO_MATCH_BEHAVIOR, GrokReader.SKIP_LINE);
        runner.enableControllerService(reader);
        runner.assertValid(reader);

        final GrokReader readerService =
                (GrokReader) runner.getProcessContext()
                        .getControllerServiceLookup()
                          .getControllerService("grok-schema-registry-service");

        assertThat(readerService, instanceOf(SchemaRegistryService.class));
    }

    @Test
    public void testSetupOfGrokReaderServiceHonorsPathExpansion() throws InitializationException {
        System.setProperty("user.home", "src/test/resources/grok");

        final TestRunner runner = TestRunners.newTestRunner(TestRegistryProcessor.class);
        final GrokReader reader = new GrokReader();

        runner.addControllerService("grok-schema-registry-service", reader);

        runner.setProperty(reader, GrokReader.PATTERN_FILE, "~/grok-pattern-file.txt");
        runner.setProperty(reader, GrokReader.GROK_EXPRESSION, "%{GREEDYDATA:message}");
        runner.setProperty(reader, GrokReader.NO_MATCH_BEHAVIOR, GrokReader.SKIP_LINE);
        runner.enableControllerService(reader);
        runner.assertValid(reader);

        final GrokReader readerService =
                (GrokReader) runner.getProcessContext()
                        .getControllerServiceLookup()
                        .getControllerService("grok-schema-registry-service");

        assertThat(readerService, instanceOf(SchemaRegistryService.class));
    }

}

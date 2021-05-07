package ${package};

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestStandardMyService {

    @BeforeEach
    public void init() {

    }

    @Test
    void testService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMyService service = new StandardMyService();
        runner.addControllerService("test-good", service);

        runner.setProperty(service, StandardMyService.MY_PROPERTY, "test-value");
        runner.enableControllerService(service);

        runner.assertValid(service);
    }

}

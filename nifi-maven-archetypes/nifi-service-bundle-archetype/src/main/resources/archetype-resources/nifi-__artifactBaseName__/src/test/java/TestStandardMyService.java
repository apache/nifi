package ${package};

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestStandardMyService {

    @Before
    public void init() {

    }

    @Test
    public void testService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMyService service = new StandardMyService();
        runner.addControllerService("test-good", service);

        runner.setProperty(service, StandardMyService.MY_PROPERTY, "test-value");
        runner.enableControllerService(service);

        runner.assertValid(service);
    }

}

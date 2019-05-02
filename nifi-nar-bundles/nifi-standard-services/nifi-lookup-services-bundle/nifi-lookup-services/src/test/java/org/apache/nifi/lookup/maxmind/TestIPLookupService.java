package org.apache.nifi.lookup.maxmind;

import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.lookup.TestProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class TestIPLookupService {

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
    public void setupIPLookupService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final IPLookupService service = new IPLookupService();

        runner.addControllerService("", service);
        runner.setProperty(service, IPLookupService.GEO_DATABASE_FILE, "src/test/resources/GeoLite2-Country.mmdb");
        runner.setProperty(service, IPLookupService.LOOKUP_ANONYMOUS_IP_INFO, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_CITY, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_CONNECTION_TYPE, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_DOMAIN, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_ISP, "false");

        runner.enableControllerService(service);
        runner.assertValid(service);

        final IPLookupService lookupService =
                (IPLookupService) runner.getProcessContext()
                  .getControllerServiceLookup()
                  .getControllerService("");

        assertThat(lookupService, instanceOf(LookupService.class));
    }

    @Test
    public void setupIPLookupServiceHonorsPathExpansion() throws InitializationException {
        System.setProperty("user.home", "src/test/resources");
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final IPLookupService service = new IPLookupService();

        runner.addControllerService("", service);
        runner.setProperty(service, IPLookupService.GEO_DATABASE_FILE, "~/GeoLite2-Country.mmdb");
        runner.setProperty(service, IPLookupService.LOOKUP_ANONYMOUS_IP_INFO, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_CITY, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_CONNECTION_TYPE, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_DOMAIN, "false");
        runner.setProperty(service, IPLookupService.LOOKUP_ISP, "false");

        runner.enableControllerService(service);
        runner.assertValid(service);

        final IPLookupService lookupService =
                (IPLookupService) runner.getProcessContext()
                        .getControllerServiceLookup()
                        .getControllerService("");

        assertThat(lookupService, instanceOf(LookupService.class));
    }

}

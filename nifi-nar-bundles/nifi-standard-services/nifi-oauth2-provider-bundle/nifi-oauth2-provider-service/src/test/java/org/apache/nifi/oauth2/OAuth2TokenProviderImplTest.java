package org.apache.nifi.oauth2;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class OAuth2TokenProviderImplTest {
    TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

            }
        });
    }

    @Test
    public void testClientCredentialGrant() {

    }

    @Test
    public void testPasswordGrant() {

    }

    @Test
    public void testRefreshToken() {

    }
}

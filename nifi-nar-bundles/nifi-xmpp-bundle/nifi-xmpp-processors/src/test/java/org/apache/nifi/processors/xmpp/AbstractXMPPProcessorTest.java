package org.apache.nifi.processors.xmpp;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import rocks.xmpp.core.XmppException;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AbstractXMPPProcessorTest {

    private TestableAbstractXMPPProcessor processor;
    private TestRunner testRunner;
    private XMPPClientSpy xmppClientSpy;

    @Before
    public void init() {
        processor = new TestableAbstractXMPPProcessor();
        testRunner = TestRunners.newTestRunner(TestableAbstractXMPPProcessor.class);
        testRunner.setProperty(AbstractXMPPProcessor.HOSTNAME, "localhost");
        testRunner.setProperty(AbstractXMPPProcessor.PORT, "5222");
        testRunner.setProperty(AbstractXMPPProcessor.XMPP_DOMAIN, "domain");
        testRunner.setProperty(AbstractXMPPProcessor.USERNAME, "user");
        testRunner.setProperty(AbstractXMPPProcessor.PASSWORD, "password");
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void createsAClientUsingTheCorrectXmppDomain() {
        testRunner.setProperty(AbstractXMPPProcessor.XMPP_DOMAIN, "domain");

        testRunner.run();

        assertThat(xmppClientSpy.xmppDomain, is("domain"));
    }

    public static class TestableAbstractXMPPProcessor extends AbstractXMPPProcessor {
        private List<PropertyDescriptor> descriptors;

        @Override
        protected void init(final ProcessorInitializationContext context) {
            this.descriptors = Collections.unmodifiableList(getBasePropertyDescriptors());
        }

        @Override
        public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return descriptors;
        }

        @Override
        public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        }

        @Override
        protected XMPPClient createXmppClient(String xmppDomain, SocketConnectionConfiguration connectionConfiguration) {
//            xmppClientSpy = new XMPPClientSpy(xmppDomain, connectionConfiguration);
            return new XMPPClientSpy(xmppDomain, connectionConfiguration);
        }
    }

    private static class XMPPClientSpy extends XMPPClientStub {
        final String xmppDomain;
        final SocketConnectionConfiguration connectionConfiguration;

        String loggedInUser;
        String providedPassword;
        String providedResource;

        boolean connectionError = false;
        boolean loginError = false;
        boolean closeError = false;

        private boolean connected = false;
        private boolean loggedIn = false;

        XMPPClientSpy(String xmppDomain, SocketConnectionConfiguration connectionConfiguration) {
            this.xmppDomain = xmppDomain;
            this.connectionConfiguration = connectionConfiguration;
        }

        @Override
        public void connect() throws XmppException {
            if (connectionError) {
                throw new XmppException("Could not connect");
            }
            connected = true;
        }

        @Override
        public void login(String user, String password, String resource) throws XmppException {
            if (!connected) {
                throw new IllegalStateException("Cannot log in when not connected");
            }
            loggedInUser = user;
            providedPassword = password;
            providedResource = resource;
            if (loginError) {
                throw new XmppException("Could not log in");
            }
            loggedIn = true;
        }

        @Override
        public void close() throws XmppException {
            if (closeError) {
                throw new XmppException("Could not close");
            }
            loggedIn = false;
            connected = false;
        }

        boolean isConnected() {
            return connected;
        }

        boolean isLoggedIn() {
            return loggedIn;
        }
    }
}

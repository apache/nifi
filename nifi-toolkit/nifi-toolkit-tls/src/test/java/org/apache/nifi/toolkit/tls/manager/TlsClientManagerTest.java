package org.apache.nifi.toolkit.tls.manager;

import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.GeneralSecurityException;

import static org.junit.Assert.*;

public class TlsClientManagerTest {

    TlsClientManager mgr;

    @Before
    public void setupTlsClientManager() throws GeneralSecurityException, IOException, InvocationTargetException, IllegalAccessException, NoSuchMethodException, ClassNotFoundException {
//        TlsClientConfig config = new TlsClientConfig(new TlsConfig());
//        config.setTrustStore("Test");
//        mgr = new TlsClientManager(config);
    }

    @Test
    public void testEscapeAliasFilenameWithForwardSlashes() throws InvocationTargetException, IllegalAccessException {
        String result = TlsClientManager.escapeAliasFilename("my/silly/filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameWithBackSlashes() throws InvocationTargetException, IllegalAccessException {
        String result = TlsClientManager.escapeAliasFilename("my\\silly\\filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameWithDollarSign() throws InvocationTargetException, IllegalAccessException {
        String result = TlsClientManager.escapeAliasFilename("my$illyfilename.pem");
        assertEquals("my_illyfilename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameSymbols() throws InvocationTargetException, IllegalAccessException {
        String result = TlsClientManager.escapeAliasFilename("./\\!@#$%^&*()_-+=.pem");
        assertEquals(".________________.pem", result);
    }


}
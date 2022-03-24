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

package org.apache.nifi.toolkit.s2s;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.KeystoreType;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SiteToSiteCliMainTest {
    private String expectedUrl;
    private TransferDirection expectedTransferDirection;
    private SiteToSiteTransportProtocol expectedSiteToSiteTransportProtocol;
    private String expectedPortName;
    private String expectedPortIdentifier;
    private long expectedTimeoutNs;
    private long expectedPenalizationNs;
    private String expectedKeystoreFilename;
    private String expectedKeystorePass;
    private KeystoreType expectedKeystoreType;
    private String expectedTruststoreFilename;
    private String expectedTruststorePass;
    private KeystoreType expectedTruststoreType;
    private boolean expectedCompression;
    private File expectedPeerPersistenceFile;
    private int expectedBatchCount;
    private long expectedBatchDuration;
    private long expectedBatchSize;
    private HttpProxy expectedHttpProxy;

    @Before
    public void setup() {
        SiteToSiteClient.Builder builder = new SiteToSiteClient.Builder();
        expectedUrl = SiteToSiteCliMain.URL_OPTION_DEFAULT;
        expectedTransferDirection = TransferDirection.valueOf(SiteToSiteCliMain.DIRECTION_OPTION_DEFAULT);
        expectedSiteToSiteTransportProtocol = SiteToSiteTransportProtocol.valueOf(SiteToSiteCliMain.TRANSPORT_PROTOCOL_OPTION_DEFAULT);
        expectedPortName = builder.getPortName();
        expectedPortIdentifier = builder.getPortIdentifier();
        expectedTimeoutNs = builder.getTimeout(TimeUnit.NANOSECONDS);
        expectedPenalizationNs = builder.getPenalizationPeriod(TimeUnit.NANOSECONDS);
        expectedKeystoreFilename = builder.getKeystoreFilename();
        expectedKeystorePass = builder.getKeystorePass();
        expectedKeystoreType = builder.getKeystoreType();
        expectedTruststoreFilename = builder.getTruststoreFilename();
        expectedTruststorePass = builder.getTruststorePass();
        expectedTruststoreType = builder.getTruststoreType();
        expectedCompression = false;
        expectedPeerPersistenceFile = builder.getPeerPersistenceFile();
        SiteToSiteClientConfig siteToSiteClientConfig = builder.buildConfig();
        expectedBatchCount = siteToSiteClientConfig.getPreferredBatchCount();
        expectedBatchDuration = siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.NANOSECONDS);
        expectedBatchSize = siteToSiteClientConfig.getPreferredBatchSize();
        expectedHttpProxy = siteToSiteClientConfig.getHttpProxy();
    }

    @Test
    public void testParseNoArgs() throws ParseException {
        parseAndCheckExpected(new String[0]);
    }

    @Test
    public void testParseUrl() throws ParseException {
        expectedUrl = "http://fake.url:8080/nifi";
        parseAndCheckExpected("u", SiteToSiteCliMain.URL_OPTION, expectedUrl);
    }

    @Test
    public void testParsePortName() throws ParseException {
        expectedPortName = "testPortName";
        parseAndCheckExpected("n", SiteToSiteCliMain.PORT_NAME_OPTION, expectedPortName);
    }

    @Test
    public void testParsePortIdentifier() throws ParseException {
        expectedPortIdentifier = "testPortId";
        parseAndCheckExpected("i", SiteToSiteCliMain.PORT_IDENTIFIER_OPTION, expectedPortIdentifier);
    }

    @Test
    public void testParseTimeout() throws ParseException {
        expectedTimeoutNs = TimeUnit.DAYS.toNanos(3);
        parseAndCheckExpected(null, SiteToSiteCliMain.TIMEOUT_OPTION, "3 days");
    }

    @Test
    public void testParsePenalization() throws ParseException {
        expectedPenalizationNs = TimeUnit.HOURS.toNanos(4);
        parseAndCheckExpected(null, SiteToSiteCliMain.PENALIZATION_OPTION, "4 hours");
    }

    @Test
    public void testParseKeystore() throws ParseException {
        expectedKeystoreFilename = "keystore.pkcs12";
        expectedKeystorePass = "badPassword";
        expectedKeystoreType = KeystoreType.PKCS12;
        parseAndCheckExpected(new String[]{
                "--" + SiteToSiteCliMain.KEYSTORE_OPTION, expectedKeystoreFilename,
                "--" + SiteToSiteCliMain.KEY_STORE_PASSWORD_OPTION, expectedKeystorePass,
                "--" + SiteToSiteCliMain.KEY_STORE_TYPE_OPTION, expectedKeystoreType.toString()
        });
    }

    @Test
    public void testParseTruststore() throws ParseException {
        expectedTruststoreFilename = "truststore.pkcs12";
        expectedTruststorePass = "badPassword";
        expectedTruststoreType = KeystoreType.PKCS12;
        parseAndCheckExpected(new String[]{
                "--" + SiteToSiteCliMain.TRUST_STORE_OPTION, expectedTruststoreFilename,
                "--" + SiteToSiteCliMain.TRUST_STORE_PASSWORD_OPTION, expectedTruststorePass,
                "--" + SiteToSiteCliMain.TRUST_STORE_TYPE_OPTION, expectedTruststoreType.toString()
        });
    }

    @Test
    public void testParseCompression() throws ParseException {
        expectedCompression = true;
        parseAndCheckExpected("c", SiteToSiteCliMain.COMPRESSION_OPTION, null);
    }

    @Test
    public void testParsePeerPersistenceFile() throws ParseException {
        String pathname = "test";
        expectedPeerPersistenceFile = new File(pathname);
        parseAndCheckExpected(null, SiteToSiteCliMain.PEER_PERSISTENCE_FILE_OPTION, pathname);
    }

    @Test
    public void testParseBatchCount() throws ParseException {
        expectedBatchCount = 55;
        parseAndCheckExpected(null, SiteToSiteCliMain.BATCH_COUNT_OPTION, Integer.toString(expectedBatchCount));
    }

    @Test
    public void testParseBatchDuration() throws ParseException {
        expectedBatchDuration = TimeUnit.MINUTES.toNanos(5);
        parseAndCheckExpected(null, SiteToSiteCliMain.BATCH_DURATION_OPTION, "5 min");
    }

    @Test
    public void testParseBatchSize() throws ParseException {
        expectedBatchSize = 1026;
        parseAndCheckExpected(null, SiteToSiteCliMain.BATCH_SIZE_OPTION, Long.toString(expectedBatchSize));
    }

    @Test
    public void testParseProxy() throws ParseException {
        String expectedHost = "testHost";
        int expectedPort = 292;
        String expectedUser = "testUser";
        String expectedPassword = "badPassword";
        expectedHttpProxy = new HttpProxy(expectedHost, expectedPort, expectedUser, expectedPassword);
        parseAndCheckExpected(new String[]{
                "--" + SiteToSiteCliMain.PROXY_HOST_OPTION, expectedHost,
                "--" + SiteToSiteCliMain.PROXY_PORT_OPTION, Integer.toString(expectedPort),
                "--" + SiteToSiteCliMain.PROXY_USERNAME_OPTION, expectedUser,
                "--" + SiteToSiteCliMain.PROXY_PASSWORD_OPTION, expectedPassword});
    }

    @Test
    public void testParseTransferDirection() throws ParseException {
        expectedTransferDirection = TransferDirection.RECEIVE;
        parseAndCheckExpected("d", SiteToSiteCliMain.DIRECTION_OPTION, expectedTransferDirection.toString());
    }

    private void parseAndCheckExpected(String shortOption, String longOption, String value) throws ParseException {
        if (shortOption != null) {
            String[] args;
            if (value == null) {
                args = new String[]{"-" + shortOption};
            } else {
                args = new String[]{"-" + shortOption, value};
            }
            parseAndCheckExpected(args);
        }
        String[] args;
        if (value == null) {
            args = new String[]{"--" + longOption};
        } else {
            args = new String[]{"--" + longOption, value};
        }
        parseAndCheckExpected(args);
    }

    private void parseAndCheckExpected(String[] args) throws ParseException {
        SiteToSiteCliMain.CliParse cliParse = SiteToSiteCliMain.parseCli(new Options(), args);
        SiteToSiteClient.Builder builder = cliParse.getBuilder();
        assertEquals(expectedUrl, builder.getUrl());
        assertEquals(expectedSiteToSiteTransportProtocol, builder.getTransportProtocol());
        assertEquals(expectedPortName, builder.getPortName());
        assertEquals(expectedPortIdentifier, builder.getPortIdentifier());
        assertEquals(expectedTimeoutNs, builder.getTimeout(TimeUnit.NANOSECONDS));
        assertEquals(expectedPenalizationNs, builder.getPenalizationPeriod(TimeUnit.NANOSECONDS));
        assertEquals(expectedKeystoreFilename, builder.getKeystoreFilename());
        assertEquals(expectedKeystorePass, builder.getKeystorePass());
        assertEquals(expectedKeystoreType, builder.getKeystoreType());
        assertEquals(expectedTruststoreFilename, builder.getTruststoreFilename());
        assertEquals(expectedTruststorePass, builder.getTruststorePass());
        assertEquals(expectedTruststoreType, builder.getTruststoreType());
        assertEquals(expectedCompression, builder.isUseCompression());
        assertEquals(expectedPeerPersistenceFile, builder.getPeerPersistenceFile());
        if (expectedHttpProxy == null) {
            assertNull(builder.getHttpProxy());
        } else {
            HttpProxy httpProxy = builder.getHttpProxy();
            assertNotNull(httpProxy);
            assertEquals(expectedHttpProxy.getHttpHost(), httpProxy.getHttpHost());
            assertEquals(expectedHttpProxy.getUsername(), httpProxy.getUsername());
            assertEquals(expectedHttpProxy.getPassword(), httpProxy.getPassword());
        }
        SiteToSiteClientConfig siteToSiteClientConfig = builder.buildConfig();
        assertEquals(expectedBatchCount, siteToSiteClientConfig.getPreferredBatchCount());
        assertEquals(expectedBatchDuration, siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.NANOSECONDS));
        assertEquals(expectedBatchSize, siteToSiteClientConfig.getPreferredBatchSize());
        assertEquals(expectedTransferDirection, cliParse.getTransferDirection());
    }
}

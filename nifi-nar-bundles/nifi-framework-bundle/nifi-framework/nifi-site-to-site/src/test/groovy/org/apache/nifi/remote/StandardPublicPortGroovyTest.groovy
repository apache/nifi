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
package org.apache.nifi.remote

import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.connectable.Connectable
import org.apache.nifi.connectable.ConnectableType
import org.apache.nifi.controller.ProcessScheduler
import org.apache.nifi.remote.protocol.CommunicationsSession
import org.apache.nifi.remote.protocol.ServerProtocol
import org.apache.nifi.reporting.BulletinRepository
import org.apache.nifi.util.NiFiProperties
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class StandardPublicPortGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StandardPublicPortGroovyTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {

    }

    @After
    void tearDown() {

    }

    private static PublicPort createPublicPort(NiFiProperties niFiProperties) {
        Authorizer mockAuthorizer = [:] as Authorizer
        BulletinRepository mockBulletinRepository = [:] as BulletinRepository
        ProcessScheduler mockProcessScheduler = [registerEvent: { Connectable worker ->
            logger.mock("Registered event for worker: ${worker}")
        }] as ProcessScheduler

        StandardPublicPort spp = new StandardPublicPort("id", "name", TransferDirection.RECEIVE, ConnectableType.INPUT_PORT, mockAuthorizer, mockBulletinRepository, mockProcessScheduler, false, niFiProperties.getBoredYieldDuration(), [])
        logger.info("Created SPP with mocked collaborators: ${spp}")
        spp
    }

    // TODO: Implement test
    @Ignore("Not yet implemented")
    @Test
    void testReceiveFlowFilesShouldHandleBlockedRequestDueToContentLength() {
        // Arrange
        Map badProps = [
                (NiFiProperties.WEB_HTTP_HOST) : "localhost",
                (NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
                (NiFiProperties.WEB_THREADS)   : NiFiProperties.DEFAULT_WEB_THREADS
        ]
        NiFiProperties mockProps = [
                getPort    : { -> 8080 },
                getSslPort : { -> 8443 },
                getProperty: { String prop ->
                    String value = badProps[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
        ] as NiFiProperties

        StandardPublicPort port = createPublicPort(mockProps)

        final int LISTEN_SECS = 5

        PeerDescription peerDescription = new PeerDescription("localhost", 8080, false)
        CommunicationsSession mockCommunicationsSession = [:] as CommunicationsSession
        Peer peer = new Peer(peerDescription, mockCommunicationsSession, "http://localhost", "")
        ServerProtocol mockServerProtocol = [getRequestExpiration: { -> 500L }] as ServerProtocol

        // Act
        port.onSchedulingStart()
        logger.info("Listening on port for ${LISTEN_SECS} seconds")
        long end = System.nanoTime() + LISTEN_SECS * 1_000_000_000
        def responses = []
        while (System.nanoTime() < end) {
            responses << port.receiveFlowFiles(peer, mockServerProtocol)
            logger.info("Received ${responses[-1]} flowfiles")
        }
        logger.info("Stopped listening on port")
        logger.info("Received ${responses.sum()} total flowfiles")

        // Assert
        assert !responses.isEmpty()
    }
}
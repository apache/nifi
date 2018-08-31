/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.processors.standard

import org.apache.nifi.security.util.crypto.HashAlgorithm
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.security.Security

@RunWith(JUnit4.class)
class CalculateHashAttributeTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CalculateHashAttributeTest.class)


    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @Test
    void testShouldHashConstantValue() throws Exception {
        // Arrange
        def algorithms = HashAlgorithm.values()
        final String knownValue = "apachenifi"

        /* These values were generated using command-line tools (openssl dgst -md5, shasum [-a 1 224 256 384 512 512224 512256], rhash --sha3-224, b2sum -l 224)
         * Ex: {@code $ echo -n "apachenifi" | openssl dgst -md5}
         */
        final def EXPECTED_HASHES = [
                md2: "25d261790198fa543b3436b4755ded91",
                md5: "a968b5ec1d52449963dcc517789baaaf",
                sha_1: "749806dbcab91a695ac85959aca610d84f03c6a7",
                sha_224: "4933803881a4ccb9b3453b829263d3e44852765db12958267ad46135",
                sha_256: "dc4bd945723b9c234f1be408e8ceb78660b481008b8ab5b71eb2aa3b4f08357a",
                sha_384: "a5205271df448e55afc4a553e91a8fea7d60d080d390d1f3484fcb6318abe94174cf3d36ea4eb1a4d5ed7637c99dec0c",
                sha_512: "0846ae23e122fbe090e94d45f886aa786acf426f56496e816a64e292b78c1bb7a962dbfd32c5c73bbee432db400970e22fd65498c862da72a305311332c6f302",
                sha_512_224: "ecf78a026035528e3097ea7289257d1819d273f60636060fbba43bfb",
                sha_512_256: "d90bdd8ad7e19f2d7848a45782d5dbe056a8213a94e03d9a35d6f44dbe7ee6cd",
                sha3_224: "2e9d1ea677847dce686ca2444cc4525f114443652fcb55af4c7286cd",
                sha3_256: "b1b3cd90a21ef60caba5ec1bf12ffcb833e52a0ae26f0ab7c4f9ccfa9c5c025b",
                sha3_384: "ca699a2447032857bf4f7e84fa316264f0c1870f9330031d5d75a0770644353c268b36d0522a3cf62e60f9401aadc37c",
                sha3_512: "cb9059d9b7ec4fde4d9710160a694e7ac2a4dd9969dee43d730066ded7b80d3eefdb4cae7622d21f6cfe16092e24f1ad6ca5924767118667654cf71b7abaaca4",
                blake2_224: "0cd14551d0ddfcc5952ee4ee4d7bab063f5c40c628296af394a02176",
                blake2_256: "40b8935dc5ed153846fb08dac8e7999ba04a74f4dab28415c39847a15c211447",
                blake2_384: "40716eddc8cfcf666d980804fed294c43fe9436a9787367a3086b45d69791fd5cef1a16c17235ea289c1e40a899b4f6b",
                blake2_512: "5f34525b130c11c469302ef6734bf6eedb1eca5d7445a3c4ae289ab58dd13ef72531966bfe2f67c4bf49c99dd14dae92d245f241482307d29bf25c45a1085026"
        ]

        CalculateAttributeHash processor = new CalculateAttributeHash()

        // Act
        def generatedHashes = algorithms.collectEntries { HashAlgorithm algorithm ->
            String hash = processor.hashValue(algorithm.name, knownValue, StandardCharsets.UTF_8)
            logger.info("${algorithm.getName().padLeft(11)}('${knownValue}') [${hash.length() / 2}] = ${hash}")
            [(algorithm.name), hash]
        }

        // Assert
        generatedHashes.each { String algorithmName, String hash ->
            String key = translateAlgorithmNameToMapKey(algorithmName)
            assert EXPECTED_HASHES[key] == hash
        }
    }

    private static String translateAlgorithmNameToMapKey(String algorithmName) {
        algorithmName.toLowerCase().replaceAll(/-\//, '_')
    }

}

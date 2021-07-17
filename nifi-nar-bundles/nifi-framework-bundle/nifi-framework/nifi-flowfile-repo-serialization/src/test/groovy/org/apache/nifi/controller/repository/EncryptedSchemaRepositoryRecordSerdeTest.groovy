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

package org.apache.nifi.controller.repository

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.controller.queue.FlowFileQueue
import org.apache.nifi.controller.repository.claim.ResourceClaimManager
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager
import org.apache.nifi.security.kms.CryptoUtils
import org.apache.nifi.security.repository.config.FlowFileRepositoryEncryptionConfiguration
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestName
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.wali.SerDe

import java.security.Security

import static org.apache.nifi.security.kms.CryptoUtils.STATIC_KEY_PROVIDER_CLASS_NAME

@RunWith(JUnit4.class)
class EncryptedSchemaRepositoryRecordSerdeTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSchemaRepositoryRecordSerdeTest.class)

    public static final String TEST_QUEUE_IDENTIFIER = "testQueueIdentifier"

    private ResourceClaimManager claimManager
    private Map<String, FlowFileQueue> queueMap
    private FlowFileQueue flowFileQueue
    private ByteArrayOutputStream byteArrayOutputStream
    private DataOutputStream dataOutputStream

    // TODO: Mock the wrapped serde
    // TODO: Make integration test with real wrapped serde
    private SerDe<RepositoryRecord> wrappedSerDe

    private static final String KPI = STATIC_KEY_PROVIDER_CLASS_NAME
    private static final String KPL = ""
    private static final String KEY_ID = "K1"
    private static final Map<String, String> KEYS = [K1: "0123456789ABCDEFFEDCBA98765432100123456789ABCDEFFEDCBA9876543210"]
    // TODO: Change to WAL impl name
    private static final String REPO_IMPL = CryptoUtils.EWAFFR_CLASS_NAME

    private FlowFileRepositoryEncryptionConfiguration flowFileREC

    private EncryptedSchemaRepositoryRecordSerde esrrs

    @Rule
    public TestName testName = new TestName()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
        claimManager = new StandardResourceClaimManager()
        queueMap = [:]
        flowFileQueue = createAndRegisterMockQueue(TEST_QUEUE_IDENTIFIER)
        byteArrayOutputStream = new ByteArrayOutputStream()
        dataOutputStream = new DataOutputStream(byteArrayOutputStream)
        wrappedSerDe = new SchemaRepositoryRecordSerde(claimManager)
        wrappedSerDe.setQueueMap(queueMap)

        flowFileREC = new FlowFileRepositoryEncryptionConfiguration(KPI, KPL, KEY_ID, KEYS, REPO_IMPL)

        esrrs = new EncryptedSchemaRepositoryRecordSerde(wrappedSerDe, flowFileREC)
    }

    @After
    void tearDown() throws Exception {
        claimManager.purge()
        queueMap.clear()
    }

    private FlowFileQueue createMockQueue(String identifier = testName.methodName + new Date().toString()) {
        [getIdentifier: { ->
            logger.mock("Retrieving flowfile queue identifier: ${identifier}")
            identifier
        }] as FlowFileQueue
    }

    private FlowFileQueue createAndRegisterMockQueue(String identifier = testName.methodName + new Date().toString()) {
        FlowFileQueue queue = createMockQueue(identifier)
        queueMap.put(identifier, queue)
        queue
    }

    private RepositoryRecord buildCreateRecord(FlowFileQueue queue, Map<String, String> attributes = [:]) {
        StandardRepositoryRecord record = new StandardRepositoryRecord(queue)
        StandardFlowFileRecord.Builder ffrb = new StandardFlowFileRecord.Builder().id(System.nanoTime())
        ffrb.addAttributes([uuid: getMockUUID()] + attributes as Map<String, String>)
        record.setWorking(ffrb.build())
        record
    }

    private String getMockUUID() {
        "${testName.methodName ?: "no_test"}@${new Date().format("mmssSSS")}" as String
    }

    /** This test ensures that the creation of a flowfile record is applied to the specified output stream correctly */
    @Test
    void testShouldSerializeAndDeserializeRecord() {
        // Arrange
        RepositoryRecord newRecord = buildCreateRecord(flowFileQueue, [id: "1", firstName: "Andy", lastName: "LoPresto"])
        DataOutputStream dos = dataOutputStream

        esrrs.writeHeader(dataOutputStream)

        // Act
        esrrs.serializeRecord(newRecord, dos)
        logger.info("Output stream: ${Hex.toHexString(byteArrayOutputStream.toByteArray())} ")

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))
        esrrs.readHeader(dis)
        RepositoryRecord deserializedRecord = esrrs.deserializeRecord(dis, 2)

        /* The records will not be identical, because the process of serializing/deserializing changes the application
         * of the delta data. The CREATE with a "current" record containing attributes becomes an UPDATE with an
         * "original" record containing attributes */

        // Assert
        logger.info("    Original record: ${newRecord.dump()}")
        logger.info("Deserialized record: ${deserializedRecord.dump()}")
        assert newRecord.type == RepositoryRecordType.CREATE
        assert deserializedRecord.type == RepositoryRecordType.UPDATE
        assert deserializedRecord.originalAttributes == newRecord.current.attributes
    }

    /** This test ensures that the creation of a flowfile record is applied to the specified output stream correctly */
    @Test
    void testShouldSerializeAndDeserializeEdit() {
        // Arrange
        RepositoryRecord newRecord = buildCreateRecord(flowFileQueue, [id: "1", firstName: "Andy", lastName: "LoPresto"])
        DataOutputStream dos = dataOutputStream

        esrrs.writeHeader(dataOutputStream)

        // Act
        esrrs.serializeEdit(null, newRecord, dos)
        logger.info("Output stream: ${Hex.toHexString(byteArrayOutputStream.toByteArray())} ")

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))
        esrrs.readHeader(dis)
        RepositoryRecord deserializedRecord = esrrs.deserializeEdit(dis, [:], 2)

        /* The records will not be identical, because the process of serializing/deserializing changes the application
         * of the delta data. The CREATE with a "current" record containing attributes becomes an UPDATE with an
         * "original" record containing attributes */

        // Assert
        logger.info("    Original record: ${newRecord.dump()}")
        logger.info("Deserialized record: ${deserializedRecord.dump()}")
        assert newRecord.type == RepositoryRecordType.CREATE
        assert deserializedRecord.type == RepositoryRecordType.UPDATE
        assert deserializedRecord.originalAttributes == newRecord.current.attributes
    }

    /** This test ensures that the creation of a flowfile record is applied to the specified output stream correctly with encryption */
    @Test
    void testShouldEncryptOutput() {
        // Arrange
        RepositoryRecord newRecord = buildCreateRecord(flowFileQueue, [id: "1", firstName: "Andy", lastName: "LoPresto"])
        DataOutputStream dos = dataOutputStream

        esrrs.writeHeader(dataOutputStream)

        // Act
        esrrs.serializeRecord(newRecord, dos)
        byte[] serializedBytes = byteArrayOutputStream.toByteArray()
        def hexOutput = Hex.toHexString(serializedBytes)
        logger.info("Output stream (${serializedBytes.length} bytes): ${hexOutput} ")

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serializedBytes))
        esrrs.readHeader(dis)
        RepositoryRecord deserializedRecord = esrrs.deserializeRecord(dis, 2)

        /* The records will not be identical, because the process of serializing/deserializing changes the application
         * of the delta data. The CREATE with a "current" record containing attributes becomes an UPDATE with an
         * "original" record containing attributes */

        // Assert
        logger.info("    Original record: ${newRecord.dump()}")
        logger.info("Deserialized record: ${deserializedRecord.dump()}")
        assert newRecord.type == RepositoryRecordType.CREATE
        assert deserializedRecord.type == RepositoryRecordType.UPDATE
        assert deserializedRecord.originalAttributes == newRecord.current.attributes

        // Ensure the value is not present in plaintext
        assert !hexOutput.contains(Hex.toHexString("Andy".bytes))

        // Ensure the value is not present in "simple" encryption (reversing bytes)
        assert !hexOutput.contains(Hex.toHexString("ydnA".bytes))

        // Ensure the encryption metadata is present in the output
        assert hexOutput.contains(Hex.toHexString("org.apache.nifi.security.repository.block.BlockEncryptionMetadata".bytes))
    }

    /** This test ensures that multiple records can be serialized and deserialized */
    @Test
    void testShouldSerializeAndDeserializeMultipleRecords() {
        // Arrange
        RepositoryRecord record1 = buildCreateRecord(flowFileQueue, [id: "1", firstName: "Andy", lastName: "LoPresto"])
        RepositoryRecord record2 = buildCreateRecord(flowFileQueue, [id: "2", firstName: "Mark", lastName: "Payne"])
        DataOutputStream dos = dataOutputStream

        // Act
        esrrs.writeHeader(dos)
        esrrs.serializeRecord(record1, dos)
        esrrs.serializeRecord(record2, dos)
        dos.flush()
        logger.info("Output stream: ${Hex.toHexString(byteArrayOutputStream.toByteArray())} ")

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))
        esrrs.readHeader(dis)
        RepositoryRecord deserializedRecord1 = esrrs.deserializeRecord(dis, esrrs.getVersion())
        RepositoryRecord deserializedRecord2 = esrrs.deserializeRecord(dis, esrrs.getVersion())

        /* The records will not be identical, because the process of serializing/deserializing changes the application
         * of the delta data. The CREATE with a "current" record containing attributes becomes an UPDATE with an
         * "original" record containing attributes */

        logger.info("Original record 1: ${record1.dump()}")
        logger.info("Original record 2: ${record2.dump()}")
        logger.info("Deserialized record 1: ${deserializedRecord1.dump()}")
        logger.info("Deserialized record 2: ${deserializedRecord2.dump()}")

        // Assert
        assert record1.type == RepositoryRecordType.CREATE
        assert record2.type == RepositoryRecordType.CREATE

        assert deserializedRecord1.type == RepositoryRecordType.UPDATE
        assert deserializedRecord1.originalAttributes == record1.current.attributes

        assert deserializedRecord2.type == RepositoryRecordType.UPDATE
        assert deserializedRecord2.originalAttributes == record2.current.attributes
    }

    /** This test ensures that multiple records can be serialized and deserialized using different keys */
    @Test
    void testShouldSerializeAndDeserializeMultipleRecordsWithMultipleKeys() {
        // Arrange
        RepositoryRecord record1 = buildCreateRecord(flowFileQueue, [id: "1", firstName: "Andy", lastName: "LoPresto"])
        RepositoryRecord record2 = buildCreateRecord(flowFileQueue, [id: "2", firstName: "Mark", lastName: "Payne"])
        DataOutputStream dos = dataOutputStream

        // Configure the serde with multiple keys available
        def multipleKeys = KEYS + [K2: "0F" * 32]
        FlowFileRepositoryEncryptionConfiguration multipleKeyFFREC = new FlowFileRepositoryEncryptionConfiguration(KPI, KPL, KEY_ID, multipleKeys, REPO_IMPL, null)

        esrrs = new EncryptedSchemaRepositoryRecordSerde(wrappedSerDe, multipleKeyFFREC)
        assert esrrs.getActiveKeyId() == "K1"

        // Act
        esrrs.writeHeader(dos)
        esrrs.serializeRecord(record1, dos)

        // Change the active key
        esrrs.setActiveKeyId("K2")
        esrrs.serializeRecord(record2, dos)
        dos.flush()
        logger.info("Output stream: ${Hex.toHexString(byteArrayOutputStream.toByteArray())} ")

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))
        esrrs.readHeader(dis)
        RepositoryRecord deserializedRecord1 = esrrs.deserializeRecord(dis, esrrs.getVersion())
        RepositoryRecord deserializedRecord2 = esrrs.deserializeRecord(dis, esrrs.getVersion())

        /* The records will not be identical, because the process of serializing/deserializing changes the application
         * of the delta data. The CREATE with a "current" record containing attributes becomes an UPDATE with an
         * "original" record containing attributes */

        logger.info("Original record 1: ${record1.dump()}")
        logger.info("Original record 2: ${record2.dump()}")
        logger.info("Deserialized record 1: ${deserializedRecord1.dump()}")
        logger.info("Deserialized record 2: ${deserializedRecord2.dump()}")

        // Assert
        assert record1.type == RepositoryRecordType.CREATE
        assert record2.type == RepositoryRecordType.CREATE

        assert deserializedRecord1.type == RepositoryRecordType.UPDATE
        assert deserializedRecord1.originalAttributes == record1.current.attributes

        assert deserializedRecord2.type == RepositoryRecordType.UPDATE
        assert deserializedRecord2.originalAttributes == record2.current.attributes
    }


}

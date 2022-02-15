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
package org.apache.nifi.controller.repository.crypto

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.controller.repository.StandardContentRepositoryContext
import org.apache.nifi.controller.repository.claim.ContentClaim
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager
import org.apache.nifi.controller.repository.util.DiskUtils
import org.apache.nifi.events.EventReporter
import org.apache.nifi.security.kms.StaticKeyProvider
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.Security

@RunWith(JUnit4.class)
class EncryptedFileSystemRepositoryTest {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedFileSystemRepositoryTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_1 = KEY_HEX_128 * 2
    private static final String KEY_ID_1 = "K1"

    private EncryptedFileSystemRepository repository = null
    private final File rootFile = new File("target/content_repository")
    private NiFiProperties nifiProperties

    private static final boolean isLossTolerant = false

    private static final String DEFAULT_NIFI_PROPS_PATH = "/conf/nifi.properties"

    private static final Map<String, String> DEFAULT_ENCRYPTION_PROPS = [
            (NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION)                              : "org.apache.nifi.controller.repository.crypto.EncryptedFileSystemRepository",
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_ID)                           : KEY_ID_1,
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY)                              : KEY_HEX_1,
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS): StaticKeyProvider.class.name,
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION)            : ""
    ]

    @BeforeClass
    static void setUpOnce() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)

        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.debug("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
        // Use mock NiFiProperties w/ encrypted configs
        repository = initializeRepository()
    }

    /**
     * Helper method to set up an encrypted content repository.
     *
     * @param nifiPropertiesPath the actual NiFi properties path
     * @param additionalProperties overriding properties for the ECR
     * @return the initialized repository
     */
    private EncryptedFileSystemRepository initializeRepository(String nifiPropertiesPath = DEFAULT_NIFI_PROPS_PATH, Map<String, String> additionalProperties = DEFAULT_ENCRYPTION_PROPS) {
        nifiProperties = NiFiProperties.createBasicNiFiProperties(EncryptedFileSystemRepositoryTest.class.getResource(nifiPropertiesPath).path, additionalProperties)
        if (rootFile.exists()) {
            DiskUtils.deleteRecursively(rootFile)
        }

        EncryptedFileSystemRepository repository = new EncryptedFileSystemRepository(nifiProperties)
        StandardResourceClaimManager claimManager = new StandardResourceClaimManager()
        repository.initialize(new StandardContentRepositoryContext(claimManager, EventReporter.NO_OP))
        repository.purge()
        logger.info("Created EFSR with nifi.properties [${nifiPropertiesPath}] and ${additionalProperties.size()} additional properties: ${additionalProperties}")

        repository
    }

    @After
    void tearDown() throws Exception {
        repository.shutdown()
    }

    @Test
    void testReadNullContentClaimShouldReturnEmptyInputStream() {
        final InputStream inputStream = repository.read((ContentClaim) null)
        final int read = inputStream.read()
        assert read == -1
    }

    /**
     * Simple test to write encrypted content to the repository, independently read the persisted file to ensure the content is encrypted, and then retrieve & decrypt via the repository.
     */
    @Test
    void testShouldEncryptAndDecrypt() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        String plainContent = "hello"
        byte[] plainBytes = plainContent.bytes
        logger.info("Writing \"${plainContent}\" (${plainContent.length()}): ${Hex.toHexString(plainBytes)}")

        // Act
        writeContentToClaim(claim, plainBytes)

        // Assert

        // Use the EFSR to decrypt the same content
        def retrievedBytes = verifyClaimDecryption(claim, plainBytes)
        assert new String(retrievedBytes, StandardCharsets.UTF_8) == plainContent
    }

    /**
     * Simple test to write encrypted image content to the repository, independently read the persisted file to ensure the content is encrypted, and then retrieve & decrypt via the repository.
     */
    @Test
    void testShouldEncryptAndDecryptImage() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File image = new File("src/test/resources/encrypted_content_repo.png")
        byte[] plainBytes = image.bytes
        logger.info("Writing \"${image.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        // Act
        writeContentToClaim(claim, plainBytes)

        // Assert

        // Use the EFSR to decrypt the same content
        verifyClaimDecryption(claim, plainBytes)
    }

    /**
     * Simple test to write multiple pieces of encrypted content to the repository and then retrieve & decrypt via the repository.
     */
    @Test
    void testShouldEncryptAndDecryptMultipleRecords() {
        def content = [
                "This is a plaintext message. ",
                "Some,csv,data\ncol1,col2,col3",
                "Easy to read 0123456789abcdef"
        ]

        def claims = createClaims(3)

        // Act
        writeContentToClaims(formClaimMap(claims, content))

        // Assert
        claims.eachWithIndex { ContentClaim claim, int i ->
            String pieceOfContent = content[i]

            // Use the EFSR to decrypt the same content
            def retrievedBytes = verifyClaimDecryption(claim, pieceOfContent.bytes)
            assert new String(retrievedBytes, StandardCharsets.UTF_8) == pieceOfContent
        }
    }

    /**
     * Simple test to show no blocking on uninitialized key ID to retrieve content.
     */
    @Test
    void testReadShouldNotRequireActiveKeyId() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        String plainContent = "hello"
        byte[] plainBytes = plainContent.bytes

        // Write the encrypted content to the repository
        writeContentToClaim(claim, plainBytes)

        // Act
        final InputStream inputStream = repository.read(claim)
        byte[] retrievedContent = inputStream.bytes
        logger.info("Read bytes via repository (${retrievedContent.length}): ${pba(retrievedContent)}")

        // Assert
        assert new String(retrievedContent, StandardCharsets.UTF_8) == plainContent
    }

    /**
     * Test to configure repository instance from nifi.properties.
     */
    @Test
    void testConstructorShouldReadFromNiFiProperties() {
        // Arrange
        String plainContent = "hello"
        byte[] plainBytes = plainContent.bytes

        // Remove the generic repository instance
        repository.purge()
        repository.cleanup()
        repository.shutdown()
        repository = null

        // Act

        // Create a new repository with the encryption properties
        repository = initializeRepository(DEFAULT_NIFI_PROPS_PATH, DEFAULT_ENCRYPTION_PROPS)

        final ContentClaim claim = repository.create(isLossTolerant)

        // Assert

        // Verify implicit configuration of necessary fields by encrypting and decrypting one record
        writeContentToClaim(claim, plainBytes)
        verifyClaimDecryption(claim, plainBytes)
    }

    /**
     * Simple test to ensure that when content is imported from an InputStream, it is encrypted.
     */
    @Test
    void testImportFromInputStreamShouldEncryptContent() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File image = new File("src/test/resources/bgBannerFoot.png")
        byte[] plainBytes = image.bytes
        logger.info("Writing \"${image.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        // Act
        final long bytesRead = repository.importFrom(image.newInputStream(), claim)
        logger.info("Read ${bytesRead} bytes from ${image.name} into ${claim.resourceClaim.id}")

        // Use the EFSR to decrypt the same content
        verifyClaimDecryption(claim, plainBytes)
    }

    /**
     * Simple test to ensure that when content is imported from a path, it is encrypted.
     */
    @Test
    void testImportFromPathShouldEncryptContent() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File image = new File("src/test/resources/bgBannerFoot.png")
        byte[] plainBytes = image.bytes
        logger.info("Writing \"${image.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        // Act
        final long bytesRead = repository.importFrom(image.toPath(), claim)
        logger.info("Read ${bytesRead} bytes from ${image.name} into ${claim.resourceClaim.id}")

        // Use the EFSR to decrypt the same content
        verifyClaimDecryption(claim, plainBytes)
    }

    /**
     * Simple test to ensure that when content is exported to an OutputStream, it is decrypted.
     */
    @Test
    void testExportToOutputStreamShouldDecryptContent() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File image = new File("src/test/resources/bgBannerFoot.png")
        byte[] plainBytes = image.bytes
        logger.info("Writing \"${image.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        writeContentToClaim(claim, plainBytes)

        final OutputStream outputStream = new ByteArrayOutputStream()

        // Act
        final long bytesWritten = repository.exportTo(claim, outputStream)
        logger.info("Wrote ${bytesWritten} bytes from ${claim.resourceClaim.id} into OutputStream")

        // Independently access the output stream and verify that the content is plain text
        byte[] exportedBytes = outputStream.toByteArray()
        logger.info("Read bytes from output stream (${exportedBytes.length}): ${pba(exportedBytes)}")

        // Assert
        assert exportedBytes == plainBytes
    }

    /**
     * Simple test to ensure that when a subset of content is exported to an OutputStream, it is decrypted.
     */
    @Test
    void testExportSubsetToOutputStreamShouldDecryptContent() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File longText = new File("src/test/resources/longtext.txt")
        byte[] plainBytes = longText.bytes
        logger.info("Writing \"${longText.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        writeContentToClaim(claim, plainBytes)

        final OutputStream outputStream = new ByteArrayOutputStream()

        // Act
        long offset = 100
        long length = 50
        logger.info("Exporting claim ${claim} (offset: ${offset}, length: ${length}) to output stream")
        logger.info("Expecting these bytes from plain content: ${pba(plainBytes[offset..<(offset + length)] as byte[])}")

        final long bytesWritten = repository.exportTo(claim, outputStream, offset, length)
        logger.info("Wrote ${bytesWritten} bytes from ${claim.resourceClaim.id} into OutputStream")

        // Independently access the output stream and verify that the content is plain text
        byte[] exportedBytes = outputStream.toByteArray()
        logger.info("Read bytes from output stream (${exportedBytes.length}): ${pba(exportedBytes)}")

        // Assert
        assert exportedBytes == plainBytes[offset..<(offset + length)] as byte[]
        assert exportedBytes.length == length
        assert bytesWritten == length
    }

    /**
     * Simple test to ensure that when content is exported to a path, it is decrypted.
     */
    @Test
    void testExportToPathShouldDecryptContent() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File image = new File("src/test/resources/bgBannerFoot.png")
        byte[] plainBytes = image.bytes
        logger.info("Writing \"${image.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        writeContentToClaim(claim, plainBytes)

        final File tempOutputFile = new File("target/exportedContent")
        final Path tempPath = tempOutputFile.toPath()

        // Act
        final long bytesWritten = repository.exportTo(claim, tempPath, false)
        logger.info("Wrote ${bytesWritten} bytes from ${claim.resourceClaim.id} into path ${tempPath}")

        // Independently access the path and verify that the content is plain text
        byte[] exportedBytes = tempOutputFile.bytes
        logger.info("Read bytes from path (${exportedBytes.length}): ${pba(exportedBytes)}")

        // Assert
        try {
            assert exportedBytes == plainBytes
        } finally {
            // Clean up
            tempOutputFile.delete()
        }
    }

    /**
     * Simple test to ensure that when a subset of content is exported to a path, it is decrypted.
     */
    @Test
    void testExportSubsetToPathShouldDecryptContent() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File longText = new File("src/test/resources/longtext.txt")
        byte[] plainBytes = longText.bytes
        logger.info("Writing \"${longText.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        writeContentToClaim(claim, plainBytes)

        final File tempOutputFile = new File("target/exportedContent")
        final Path tempPath = tempOutputFile.toPath()

        // Act
        long offset = 100
        long length = 50
        logger.info("Exporting claim ${claim} (offset: ${offset}, length: ${length}) to output stream")
        logger.info("Expecting these bytes from plain content: ${pba(plainBytes[offset..<(offset + length)] as byte[])}")

        final long bytesWritten = repository.exportTo(claim, tempPath, false, offset, length)
        logger.info("Wrote ${bytesWritten} bytes from ${claim.resourceClaim.id} into path ${tempPath}")

        // Independently access the path and verify that the content is plain text
        byte[] exportedBytes = tempOutputFile.bytes
        logger.info("Read bytes from path (${exportedBytes.length}): ${pba(exportedBytes)}")

        // Assert
        try {
            assert exportedBytes == plainBytes[offset..<(offset + length)] as byte[]
            assert exportedBytes.length == length
            assert bytesWritten == length
        } finally {
            // Clean up
            tempOutputFile.delete()
        }
    }

    /**
     * Simple test to clone encrypted content claim and ensure that the cloned encryption metadata accurately reflects the new claim and allows for decryption.
     */
    @Test
    void testCloneShouldUpdateEncryptionMetadata() {
        // Arrange
        final ContentClaim claim = repository.create(isLossTolerant)

        File textFile = new File("src/test/resources/longtext.txt")
        byte[] plainBytes = textFile.bytes
        logger.info("Writing \"${textFile.name}\" (${plainBytes.length}): ${pba(plainBytes)}")

        // Write to the content repository (encrypted)
        writeContentToClaim(claim, plainBytes)

        // Clone the content claim
        logger.info("Preparing to clone claim ${claim}")
        ContentClaim clonedClaim = repository.clone(claim, isLossTolerant)
        logger.info("Cloned claim ${claim} to ${clonedClaim}")

        // Use the EFSR to decrypt the original claim content
        def retrievedOriginalBytes = verifyClaimDecryption(claim, plainBytes)
        assert retrievedOriginalBytes == plainBytes

        // Use the EFSR to decrypt the cloned claim content
        def retrievedClonedBytes = verifyClaimDecryption(clonedClaim, plainBytes)
        assert retrievedClonedBytes == plainBytes
    }

    /**
     * Simple test to merge two encrypted content claims and ensure that the merged encryption metadata accurately reflects the new claim and allows for decryption.
     */
    @Test
    void testMergeShouldUpdateEncryptionMetadata() {
        // Arrange
        int claimCount = 2
        def claims = createClaims(claimCount, isLossTolerant)

        File textFile = new File("src/test/resources/longtext.txt")
        byte[] plainBytes = textFile.bytes
        String plainContent = textFile.text

        // Split the long text into two claims
        def content = splitTextIntoSections(plainContent, claimCount)

        // Write each piece of content to the respective claim
        writeContentToClaims(formClaimMap(claims, content))

        // Merge the two content claims
        logger.info("Preparing to merge claims ${claims}")
        ContentClaim mergedClaim = repository.create(isLossTolerant)
        // The header, footer, and demarcator are null in this case
        long bytesWrittenDuringMerge = repository.merge(claims, mergedClaim, null, null, null)
        logger.info("Merged ${claims.size()} claims (${bytesWrittenDuringMerge} bytes) to ${mergedClaim}")

        // Use the EFSR to decrypt the original claims content
        claims.eachWithIndex { ContentClaim claim, int i ->
            verifyClaimDecryption(claim, content[i].bytes)
        }

        // Use the EFSR to decrypt the merged claim content
        verifyClaimDecryption(mergedClaim, plainBytes, "merged")
    }

    /**
     * Simple test to merge encrypted content claims and ensure that the merged encryption metadata accurately reflects the new claim and allows for decryption, including the header, demarcator, and footer.
     */
    @Test
    void testMergeWithMarkersShouldUpdateEncryptionMetadata() {
        // Arrange
        int claimCount = 4
        def claims = createClaims(claimCount, isLossTolerant)

        File textFile = new File("src/test/resources/longtext.txt")
        String plainContent = textFile.text

        // Split the long text into two claims
        List<String> content = splitTextIntoSections(plainContent, claimCount)

        // Write each piece of content to the respective claim
        writeContentToClaims(formClaimMap(claims, content))

        // Define the markers
        String header = "---Header---\n"
        String demarcator = "\n---Boundary---\n"
        String footer = "\n---Footer---"
        final String EXPECTED_MERGED_CONTENT = header + content.join(demarcator) + footer

        // Merge the content claims
        logger.info("Preparing to merge claims ${claims}")
        ContentClaim mergedClaim = repository.create(isLossTolerant)
        // The header, footer, and demarcator are populated in this case
        long bytesWrittenDuringMerge = repository.merge(claims, mergedClaim, header.bytes, footer.bytes, demarcator.bytes)
        logger.info("Merged ${claims.size()} claims (${bytesWrittenDuringMerge} bytes) to ${mergedClaim}")

        // Use the EFSR to decrypt the original claims content
        claims.eachWithIndex { ContentClaim claim, int i ->
            verifyClaimDecryption(claim, content[i].bytes)
        }

        // Use the EFSR to decrypt the merged claim content
        verifyClaimDecryption(mergedClaim, EXPECTED_MERGED_CONTENT.bytes, "merged")
    }

    /**
     * Returns a {@code List<String>} with length {@code N}, where N is the number of elements requested. Each element
     * will be roughly the same size.
     *
     * @param plainContent the original String content
     * @param requestedElements the number of pieces of content to return
     * @return a list containing {@code requestedElements} elements
     */
    private static List<String> splitTextIntoSections(String plainContent, int requestedElements = 2) {
        Number contentSectionLength = plainContent.size().intdiv(requestedElements)
        def content = []
        int start, end = 0
        requestedElements.times { int i ->
            start = i * contentSectionLength
            end = (i + 1) * contentSectionLength
            content << plainContent[start..<end]
        }
        content
    }

    /**
     * Helper method to verify the provided claim content equals the expected plain content,
     * decrypted via the {@link EncryptedFileSystemRepository#read()} method.
     *
     * @param claim the claim to verify
     * @param plainBytes the expected content once decrypted
     * @param description a message for contextualized log output
     * @return the retrieved, decrypted bytes
     */
    private byte[] verifyClaimDecryption(ContentClaim claim, byte[] plainBytes, String description = "claim") {
        final InputStream inputStream = repository.read(claim)
        byte[] retrievedBytes = inputStream.bytes
        logger.info("Read ${description} bytes via repository (${retrievedBytes.length}): ${pba(retrievedBytes)}")

        // Assert
        assert retrievedBytes == plainBytes
        return retrievedBytes
    }

    /**
     * Helper method to create <em>n</em> claims.
     *
     * @param n the number of claims to create
     * @param isLossTolerant true if the claims are loss tolerant
     * @return the list of claims
     */
    private List<ContentClaim> createClaims(int n, boolean isLossTolerant = false) {
        def claims = []
        n.times {
            claims << repository.create(isLossTolerant)
        }
        claims
    }

    /**
     * Helper method to form a map out of parallel lists of claims and their respective
     * content (converts to bytes if in a String).
     *
     * @param claims the list of claims
     * @param content the list of content (indexed in the same order)
     * @return the map of the claims and content
     */
    private static Map<ContentClaim, byte[]> formClaimMap(List<ContentClaim> claims, List content) {
        def claimMap = [:]
        claims.eachWithIndex { ContentClaim claim, int i ->
            def element = content[i]
            claimMap << [(claim): element instanceof byte[] ? element : element.bytes]
        }
        claimMap
    }

    /**
     * Helper method to iterate over a map of claim -> byte[] content and write it out.
     *
     * @param claimsAndContent a map of claims and their respective incoming content
     */
    private void writeContentToClaims(Map<ContentClaim, byte[]> claimsAndContent) {
        claimsAndContent.each { ContentClaim claim, byte[] content ->
            writeContentToClaim(claim, content)
        }
    }

    /**
     * Helper method to write the content to a claim.
     *
     * @param claim the claim
     * @param content the byte[] to write
     */
    private void writeContentToClaim(ContentClaim claim, byte[] content) {
        // Write to the content repository (encrypted)
        final OutputStream out = repository.write(claim)
        out.write(content)
        out.flush()
        out.close()
    }

    /**
     * Returns a truncated byte[] in hexadecimal encoding as a String.
     *
     * @param bytes the byte[]
     * @param length the length in bytes to show (default 16)
     * @return the hex-encoded representation of {@code length} bytes
     */
    private static String pba(byte[] bytes, int length = 16) {
        "[${Hex.toHexString(bytes)[0..<(Math.min(length, bytes.length * 2))]}${bytes.length * 2 > length ? "..." : ""}]"
    }
}

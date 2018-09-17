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
package org.apache.nifi.security.util.crypto

import org.apache.nifi.components.AllowableValue
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.security.Security

@RunWith(JUnit4.class)
class HashServiceTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(HashServiceTest.class)

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @AfterClass
    static void tearDownOnce() throws Exception {
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @Test
    void testShouldHashValue() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256
        final String KNOWN_VALUE = "apachenifi"

        final String EXPECTED_HASH = "dc4bd945723b9c234f1be408e8ceb78660b481008b8ab5b71eb2aa3b4f08357a"
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH)

        Closure threeArgString = { -> HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8) }
        Closure twoArgString = { -> HashService.hashValue(algorithm, KNOWN_VALUE) }
        Closure threeArgStringRaw = { -> HashService.hashValueRaw(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8) }
        Closure twoArgStringRaw = { -> HashService.hashValueRaw(algorithm, KNOWN_VALUE) }
        Closure twoArgBytesRaw = { -> HashService.hashValueRaw(algorithm, KNOWN_VALUE.bytes) }

        def scenarios = [threeArgString   : threeArgString,
                         twoArgString     : twoArgString,
                         threeArgStringRaw: threeArgStringRaw,
                         twoArgStringRaw  : twoArgStringRaw,
                         twoArgBytesRaw   : twoArgBytesRaw,
        ]

        // Act
        scenarios.each { String name, Closure closure ->
            def result = closure.call()
            logger.info("${name.padLeft(20)}: ${result.class.simpleName.padLeft(8)} ${result}")

            // Assert
            if (result instanceof byte[]) {
                assert result == EXPECTED_HASH_BYTES
            } else {
                assert result == EXPECTED_HASH
            }
        }
    }

    @Test
    void testHashValueShouldDifferOnDifferentEncodings() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256
        final String KNOWN_VALUE = "apachenifi"

        // Act
        String utf8Hash = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8)
        logger.info(" UTF-8: ${utf8Hash}")
        String utf16Hash = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_16)
        logger.info("UTF-16: ${utf16Hash}")

        // Assert
        assert utf8Hash != utf16Hash
    }

    /**
     * This test ensures that the service properly handles UTF-16 encoded data to return it without the Big Endian Byte Order Mark (BOM). Java treats UTF-16 encoded data without a BOM as Big Endian by default on decoding, but when <em>encoding</em>, it inserts a BE BOM in the data.
     *
     * Examples:
     *
     * "apachenifi"
     *
     * *     UTF-8: 0x61 0x70 0x61 0x63 0x68 0x65 0x6E 0x69 0x66 0x69
     * *    UTF-16: 0xFE 0xFF 0x00 0x61 0x00 0x70 0x00 0x61 0x00 0x63 0x00 0x68 0x00 0x65 0x00 0x6E 0x00 0x69 0x00 0x66 0x00 0x69
     * *  UTF-16LE: 0x61 0x00 0x70 0x00 0x61 0x00 0x63 0x00 0x68 0x00 0x65 0x00 0x6E 0x00 0x69 0x00 0x66 0x00 0x69 0x00
     * *  UTF-16BE: 0x00 0x61 0x00 0x70 0x00 0x61 0x00 0x63 0x00 0x68 0x00 0x65 0x00 0x6E 0x00 0x69 0x00 0x66 0x00 0x69
     *
     * The result of "UTF-16" decoding should have the 0xFE 0xFF stripped on return by encoding in UTF-16BE directly, which will not insert a BOM.
     *
     * See also: <a href="https://unicode.org/faq/utf_bom.html#bom10">https://unicode.org/faq/utf_bom.html#bom10</a>
     */
    @Test
    void testHashValueShouldHandleUTF16BOMIssue() {
        // Arrange
        HashAlgorithm algorithm = HashAlgorithm.SHA256
        final String KNOWN_VALUE = "apachenifi"

        List<Charset> charsets = [StandardCharsets.UTF_8, StandardCharsets.UTF_16, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE]

        charsets.each { Charset charset ->
            logger.info("[${charset.name().padLeft(9)}]: ${printHexBytes(KNOWN_VALUE, charset)}")
        }

        final def EXPECTED_SHA_256_HASHES = [
                "utf_8"   : "dc4bd945723b9c234f1be408e8ceb78660b481008b8ab5b71eb2aa3b4f08357a",
                "utf_16"  : "f370019c2a41a8285077beb839f7566240e2f0ca970cb67aed5836b89478df91",
                "utf_16be": "f370019c2a41a8285077beb839f7566240e2f0ca970cb67aed5836b89478df91",
                "utf_16le": "7e285dc64d3a8c3cb4e04304577eebbcb654f2245373874e48e597a8b8f15aff",
        ]
        EXPECTED_SHA_256_HASHES.each { k, hash ->
            logger.expected("SHA-256(${k.padLeft(9)}(${KNOWN_VALUE})) = ${hash}")
        }

        // Act
        charsets.each { Charset charset ->
            // Calculate the expected hash value given the character set
            String hash = HashService.hashValue(algorithm, KNOWN_VALUE, charset)
            logger.info("${algorithm.name}(${KNOWN_VALUE}, ${charset.name().padLeft(9)}) = ${hash}")

            // Assert
            assert hash == EXPECTED_SHA_256_HASHES[translateStringToMapKey(charset.name())]
        }
    }

    @Test
    void testHashValueShouldDefaultToUTF8() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256
        final String KNOWN_VALUE = "apachenifi"

        // Act
        String explicitUTF8Hash = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8)
        logger.info("Explicit UTF-8: ${explicitUTF8Hash}")
        String implicitUTF8Hash = HashService.hashValue(algorithm, KNOWN_VALUE)
        logger.info("Implicit UTF-8: ${implicitUTF8Hash}")

        byte[] explicitUTF8HashBytes = HashService.hashValueRaw(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8)
        logger.info("Explicit UTF-8 bytes: ${explicitUTF8HashBytes}")
        byte[] implicitUTF8HashBytes = HashService.hashValueRaw(algorithm, KNOWN_VALUE)
        logger.info("Implicit UTF-8 bytes: ${implicitUTF8HashBytes}")
        byte[] implicitUTF8HashBytesDefault = HashService.hashValueRaw(algorithm, KNOWN_VALUE.bytes)
        logger.info("Implicit UTF-8 bytes: ${implicitUTF8HashBytesDefault}")

        // Assert
        assert explicitUTF8Hash == implicitUTF8Hash
        assert explicitUTF8HashBytes == implicitUTF8HashBytes
        assert explicitUTF8HashBytes == implicitUTF8HashBytesDefault
    }

    @Test
    void testShouldRejectNullAlgorithm() {
        // Arrange
        final String KNOWN_VALUE = "apachenifi"

        Closure threeArgString = { -> HashService.hashValue(null, KNOWN_VALUE, StandardCharsets.UTF_8) }
        Closure twoArgString = { -> HashService.hashValue(null, KNOWN_VALUE) }
        Closure threeArgStringRaw = { -> HashService.hashValueRaw(null, KNOWN_VALUE, StandardCharsets.UTF_8) }
        Closure twoArgStringRaw = { -> HashService.hashValueRaw(null, KNOWN_VALUE) }
        Closure twoArgBytesRaw = { -> HashService.hashValueRaw(null, KNOWN_VALUE.bytes) }

        def scenarios = [threeArgString   : threeArgString,
                         twoArgString     : twoArgString,
                         threeArgStringRaw: threeArgStringRaw,
                         twoArgStringRaw  : twoArgStringRaw,
                         twoArgBytesRaw   : twoArgBytesRaw,
        ]

        // Act
        scenarios.each { String name, Closure closure ->
            def msg = shouldFail(IllegalArgumentException) {
                closure.call()
            }
            logger.expected("${name.padLeft(20)}: ${msg}")

            // Assert
            assert msg =~ "The hash algorithm cannot be null"
        }
    }

    @Test
    void testShouldRejectNullValue() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256

        Closure threeArgString = { -> HashService.hashValue(algorithm, null, StandardCharsets.UTF_8) }
        Closure twoArgString = { -> HashService.hashValue(algorithm, null) }
        Closure threeArgStringRaw = { -> HashService.hashValueRaw(algorithm, null, StandardCharsets.UTF_8) }
        Closure twoArgStringRaw = { -> HashService.hashValueRaw(algorithm, null as String) }
        Closure twoArgBytesRaw = { -> HashService.hashValueRaw(algorithm, null as byte[]) }

        def scenarios = [threeArgString   : threeArgString,
                         twoArgString     : twoArgString,
                         threeArgStringRaw: threeArgStringRaw,
                         twoArgStringRaw  : twoArgStringRaw,
                         twoArgBytesRaw   : twoArgBytesRaw,
        ]

        // Act
        scenarios.each { String name, Closure closure ->
            def msg = shouldFail(IllegalArgumentException) {
                closure.call()
            }
            logger.expected("${name.padLeft(20)}: ${msg}")

            // Assert
            assert msg =~ "The value cannot be null"
        }
    }

    @Test
    void testShouldHashConstantValue() throws Exception {
        // Arrange
        def algorithms = HashAlgorithm.values()
        final String KNOWN_VALUE = "apachenifi"

        /* These values were generated using command-line tools (openssl dgst -md5, shasum [-a 1 224 256 384 512 512224 512256], rhash --sha3-224, b2sum -l 224)
         * Ex: {@code $ echo -n "apachenifi" | openssl dgst -md5}
         */
        final def EXPECTED_HASHES = [
                md2        : "25d261790198fa543b3436b4755ded91",
                md5        : "a968b5ec1d52449963dcc517789baaaf",
                sha_1      : "749806dbcab91a695ac85959aca610d84f03c6a7",
                sha_224    : "4933803881a4ccb9b3453b829263d3e44852765db12958267ad46135",
                sha_256    : "dc4bd945723b9c234f1be408e8ceb78660b481008b8ab5b71eb2aa3b4f08357a",
                sha_384    : "a5205271df448e55afc4a553e91a8fea7d60d080d390d1f3484fcb6318abe94174cf3d36ea4eb1a4d5ed7637c99dec0c",
                sha_512    : "0846ae23e122fbe090e94d45f886aa786acf426f56496e816a64e292b78c1bb7a962dbfd32c5c73bbee432db400970e22fd65498c862da72a305311332c6f302",
                sha_512_224: "ecf78a026035528e3097ea7289257d1819d273f60636060fbba43bfb",
                sha_512_256: "d90bdd8ad7e19f2d7848a45782d5dbe056a8213a94e03d9a35d6f44dbe7ee6cd",
                sha3_224   : "2e9d1ea677847dce686ca2444cc4525f114443652fcb55af4c7286cd",
                sha3_256   : "b1b3cd90a21ef60caba5ec1bf12ffcb833e52a0ae26f0ab7c4f9ccfa9c5c025b",
                sha3_384   : "ca699a2447032857bf4f7e84fa316264f0c1870f9330031d5d75a0770644353c268b36d0522a3cf62e60f9401aadc37c",
                sha3_512   : "cb9059d9b7ec4fde4d9710160a694e7ac2a4dd9969dee43d730066ded7b80d3eefdb4cae7622d21f6cfe16092e24f1ad6ca5924767118667654cf71b7abaaca4",
                blake2_160 : "7bc5a408dba4f1934d9090c4d75c65bfa0c7c90c",
                blake2_256 : "40b8935dc5ed153846fb08dac8e7999ba04a74f4dab28415c39847a15c211447",
                blake2_384 : "40716eddc8cfcf666d980804fed294c43fe9436a9787367a3086b45d69791fd5cef1a16c17235ea289c1e40a899b4f6b",
                blake2_512 : "5f34525b130c11c469302ef6734bf6eedb1eca5d7445a3c4ae289ab58dd13ef72531966bfe2f67c4bf49c99dd14dae92d245f241482307d29bf25c45a1085026"
        ]

        // Act
        def generatedHashes = algorithms.collectEntries { HashAlgorithm algorithm ->
            String hash = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8)
            logger.info("${algorithm.getName().padLeft(11)}('${KNOWN_VALUE}') [${hash.length() / 2}] = ${hash}")
            [(algorithm.name), hash]
        }

        // Assert
        generatedHashes.each { String algorithmName, String hash ->
            String key = translateStringToMapKey(algorithmName)
            assert EXPECTED_HASHES[key] == hash
        }
    }

    @Test
    void testShouldHashEmptyValue() throws Exception {
        // Arrange
        def algorithms = HashAlgorithm.values()
        final String EMPTY_VALUE = ""

        /* These values were generated using command-line tools (openssl dgst -md5, shasum [-a 1 224 256 384 512 512224 512256], rhash --sha3-224, b2sum -l 224)
         * Ex: {@code $ echo -n "" | openssl dgst -md5}
         */
        final def EXPECTED_HASHES = [
                md2        : "8350e5a3e24c153df2275c9f80692773",
                md5        : "d41d8cd98f00b204e9800998ecf8427e",
                sha_1      : "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                sha_224    : "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
                sha_256    : "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                sha_384    : "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
                sha_512    : "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
                sha_512_224: "6ed0dd02806fa89e25de060c19d3ac86cabb87d6a0ddd05c333b84f4",
                sha_512_256: "c672b8d1ef56ed28ab87c3622c5114069bdd3ad7b8f9737498d0c01ecef0967a",
                sha3_224   : "6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7",
                sha3_256   : "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
                sha3_384   : "0c63a75b845e4f7d01107d852e4c2485c51a50aaaa94fc61995e71bbee983a2ac3713831264adb47fb6bd1e058d5f004",
                sha3_512   : "a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26",
                blake2_160 : "3345524abf6bbe1809449224b5972c41790b6cf2",
                blake2_256 : "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8",
                blake2_384 : "b32811423377f52d7862286ee1a72ee540524380fda1724a6f25d7978c6fd3244a6caf0498812673c5e05ef583825100",
                blake2_512 : "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce"
        ]

        // Act
        def generatedHashes = algorithms.collectEntries { HashAlgorithm algorithm ->
            String hash = HashService.hashValue(algorithm, EMPTY_VALUE, StandardCharsets.UTF_8)
            logger.info("${algorithm.getName().padLeft(11)}('${EMPTY_VALUE}') [${hash.length() / 2}] = ${hash}")
            [(algorithm.name), hash]
        }

        // Assert
        generatedHashes.each { String algorithmName, String hash ->
            String key = translateStringToMapKey(algorithmName)
            assert EXPECTED_HASHES[key] == hash
        }
    }

    @Test
    void testShouldBuildHashAlgorithmAllowableValues() throws Exception {
        // Arrange
        final def EXPECTED_ALGORITHMS = HashAlgorithm.values()
        logger.info("The consistent list of hash algorithms available [${EXPECTED_ALGORITHMS.size()}]: \n${EXPECTED_ALGORITHMS.collect { "\t${it.name}" }.join("\n")}")

        // Act
        def allowableValues = HashService.buildHashAlgorithmAllowableValues()

        // Assert
        assert allowableValues instanceof AllowableValue[]

        def valuesList = allowableValues as List<AllowableValue>
        assert valuesList.size() == EXPECTED_ALGORITHMS.size()
        EXPECTED_ALGORITHMS.each { HashAlgorithm expectedAlgorithm ->
            def matchingValue = valuesList.find { it.value == expectedAlgorithm.name }
            assert matchingValue.displayName == expectedAlgorithm.name
            assert matchingValue.description == expectedAlgorithm.buildAllowableValueDescription()
        }
    }

    @Test
    void testShouldBuildCharacterSetAllowableValues() throws Exception {
        // Arrange
        final def EXPECTED_CHARACTER_SETS = [
                StandardCharsets.US_ASCII,
                StandardCharsets.ISO_8859_1,
                StandardCharsets.UTF_8,
                StandardCharsets.UTF_16BE,
                StandardCharsets.UTF_16LE,
                StandardCharsets.UTF_16,
        ]
        logger.info("The consistent list of character sets available [${EXPECTED_CHARACTER_SETS.size()}]: \n${EXPECTED_CHARACTER_SETS.collect { "\t${it.name()}" }.join("\n")}")

        def expectedDescriptions = ["UTF-16": "This character set normally decodes using an optional BOM at the beginning of the data but encodes by inserting a BE BOM. For hashing, it will be replaced with UTF-16BE. "]

        // Act
        def allowableValues = HashService.buildCharacterSetAllowableValues()

        // Assert
        assert allowableValues instanceof AllowableValue[]

        def valuesList = allowableValues as List<AllowableValue>
        assert valuesList.size() == EXPECTED_CHARACTER_SETS.size()
        EXPECTED_CHARACTER_SETS.each { Charset charset ->
            def matchingValue = valuesList.find { it.value == charset.name() }
            assert matchingValue.displayName == charset.name()
            assert matchingValue.description == (expectedDescriptions[charset.name()] ?: charset.displayName())
        }
    }

    @Test
    void testShouldHashValueFromStream() throws Exception {
        // Arrange

        // No command-line md2sum tool available
        def algorithms = HashAlgorithm.values() - HashAlgorithm.MD2

        File inputFile = temporaryFolder.newFile()

        // Generates a file with "apachenifi" 10 times per line for 10_000 lines (11 bytes * 10 * 10_000 ~= 1 MiB)
        if (!inputFile.exists() || inputFile.length() == 0) {
            inputFile.createNewFile()
            10_000.times { int i ->
                inputFile << "${i.toString().padLeft(5)}: ${"apachenifi " * 10}\n"
            }
        }

        /* These values were generated using command-line tools (openssl dgst -md5, shasum [-a 1 224 256 384 512 512224 512256], rhash --sha3-224, b2sum -l 160)
         * Ex: {@code $ openssl dgst -md5 src/test/resources/HashServiceTest/largefile.txt}
         */
        final def EXPECTED_HASHES = [
                md5        : "8d329076847b678449610a5fb53997d2",
                sha_1      : "09cd981ee7529cfd6268a69c0d53e8117e9c78b1",
                sha_224    : "4d4d58c226959e0775e627a866eaa26bf18121d578b559946aea6f8c",
                sha_256    : "ce50f183a8011a86c5162e94481c6b14ad921a8001746806063b3033e71440eb",
                sha_384    : "62a13a410566856422f0b81b2e6ab26f91b3da1a877a5c24f681d2812f26abbc43fb637954879915b3cd9aad626ca71c",
                sha_512    : "3f036116c78b1d9e2017bb1fd4b04f449839e6434c94442edebffdcdfbac1d79b483978126f0ffb12824f14ecc36a07dc95f0ba04aa68885456f3f6381471e07",
                sha_512_224: "aa7227a80889366a2325801a5cfa67f29c8f272f4284aecfe5daba3c",
                sha_512_256: "76faa424ee31bcb1f3a41a848806e288cb064a6bf1867881ee1b439dd8b38e40",
                sha3_224   : "d4bb36bf2d00117ade2e63c6fa2ef5f6714d8b6c7a40d12623f95fd0",
                sha3_256   : "f93ff4178bc7f466444a822191e152332331ba51eee42b952b3be1b46b1921f7",
                sha3_384   : "7e4dfb0073645f059e5837f7c066bffd7f8b5d888b0179a8f0be6bb11c7d631847c468d4d861abcdc96503d91f2a7a78",
                sha3_512   : "bf8e83f3590727e04777406e1d478615cf68468ad8690dba3f22a879e08022864a2b4ad8e8a1cbc88737578abd4b2e8493e3bda39a81af3f21fc529c1a7e3b52",
                blake2_160 : "71dd4324a1f72aa10aaa59ee4d79ceee8d8915e6",
                blake2_256 : "5a25864c69f42adeefc343989babb6972df38da47bb6ce712fbef4474266b539",
                blake2_384 : "52417243317ca01693ba835bd5d6655c73a2f70d811b4d26ddacf9e3b74fc3993f30adc64fb6c23a6a5c1e36771a0b95",
                blake2_512 : "be81dbc396a9e11c6189d2408a956466fb1c784d2d34495f9ca43434041b425675005deaeea1a04b1f44db0200b19cde5a40fd5e88414bb300620bc3d5e30f6a"
        ]

        // Act
        def generatedHashes = algorithms.collectEntries { HashAlgorithm algorithm ->
            // Get a new InputStream for each iteration, or it will calculate the hash of an empty input on iterations 1 - n
            InputStream input = inputFile.newInputStream()
            String hash = HashService.hashValueStreaming(algorithm, input)
            logger.info("${algorithm.getName().padLeft(11)}(${inputFile.path}) [${hash.length() / 2}] = ${hash}")
            [(algorithm.name), hash]
        }

        // Assert
        generatedHashes.each { String algorithmName, String hash ->
            String key = translateStringToMapKey(algorithmName)
            assert EXPECTED_HASHES[key] == hash
        }
    }

    /**
     * Returns a {@link String} containing the hex-encoded bytes in the format "0xAB 0xCD ...".
     *
     * @param data the String to convert
     * @param charset the {@link Charset} to use
     * @return the formatted string
     */
    private static String printHexBytes(String data, Charset charset) {
        data.getBytes(charset).collect { "0x${Hex.toHexString([it] as byte[]).toUpperCase()}" }.join(" ")
    }

    private static String translateStringToMapKey(String string) {
        string.toLowerCase().replaceAll(/[-\/]/, '_')
    }
}

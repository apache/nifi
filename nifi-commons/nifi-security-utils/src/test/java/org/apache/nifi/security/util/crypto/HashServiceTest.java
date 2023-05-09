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
package org.apache.nifi.security.util.crypto;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashServiceTest {
    private static final String KNOWN_VALUE = "apachenifi";

    @Test
    void testShouldHashValue() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256;

        final String EXPECTED_HASH = "dc4bd945723b9c234f1be408e8ceb78660b481008b8ab5b71eb2aa3b4f08357a";
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH);

        String threeArgString = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8);
        String twoArgString = HashService.hashValue(algorithm, KNOWN_VALUE);
        byte[] threeArgStringRaw = HashService.hashValueRaw(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8);
        byte[] twoArgStringRaw = HashService.hashValueRaw(algorithm, KNOWN_VALUE);
        byte[] twoArgBytesRaw = HashService.hashValueRaw(algorithm, KNOWN_VALUE.getBytes());

        final Map<String, Object> scenarios = new HashMap<>();
        scenarios.put("threeArgString", threeArgString);
        scenarios.put("twoArgString", twoArgString);
        scenarios.put("threeArgStringRaw", threeArgStringRaw);
        scenarios.put("twoArgStringRaw", twoArgStringRaw);
        scenarios.put("twoArgBytesRaw", twoArgBytesRaw);

        // Act
        for (final Object result : scenarios.values()) {
            // Assert
            if (result instanceof byte[]) {
                assertArrayEquals(EXPECTED_HASH_BYTES, (byte[]) result);
            } else {
                assertEquals(EXPECTED_HASH, result);
            }
        }
    }

    @Test
    void testHashValueShouldDifferOnDifferentEncodings() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256;

        // Act
        String utf8Hash = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8);
        String utf16Hash = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_16);

        // Assert
        assertNotEquals(utf8Hash, utf16Hash);
    }

    /**
     * This test ensures that the service properly handles UTF-16 encoded data to return it without
     * the Big Endian Byte Order Mark (BOM). Java treats UTF-16 encoded data without a BOM as Big Endian by default on decoding, but when <em>encoding</em>, it inserts a BE BOM in the data.
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
        HashAlgorithm algorithm = HashAlgorithm.SHA256;

        List<Charset> charsets = Arrays.asList(StandardCharsets.UTF_8, StandardCharsets.UTF_16, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE);

        final Map<String, String> EXPECTED_SHA_256_HASHES = new HashMap<>();
        EXPECTED_SHA_256_HASHES.put("utf_8", "dc4bd945723b9c234f1be408e8ceb78660b481008b8ab5b71eb2aa3b4f08357a");
        EXPECTED_SHA_256_HASHES.put("utf_16", "f370019c2a41a8285077beb839f7566240e2f0ca970cb67aed5836b89478df91");
        EXPECTED_SHA_256_HASHES.put("utf_16be", "f370019c2a41a8285077beb839f7566240e2f0ca970cb67aed5836b89478df91");
        EXPECTED_SHA_256_HASHES.put("utf_16le", "7e285dc64d3a8c3cb4e04304577eebbcb654f2245373874e48e597a8b8f15aff");

        // Act
        for (final Charset charset : charsets) {
            // Calculate the expected hash value given the character set
            String hash = HashService.hashValue(algorithm, KNOWN_VALUE, charset);

            // Assert
            assertEquals(EXPECTED_SHA_256_HASHES.get(translateStringToMapKey(charset.name())), hash);
        }
    }

    @Test
    void testHashValueShouldDefaultToUTF8() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256;

        // Act
        String explicitUTF8Hash = HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8);
        String implicitUTF8Hash = HashService.hashValue(algorithm, KNOWN_VALUE);

        byte[] explicitUTF8HashBytes = HashService.hashValueRaw(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8);
        byte[] implicitUTF8HashBytes = HashService.hashValueRaw(algorithm, KNOWN_VALUE);
        byte[] implicitUTF8HashBytesDefault = HashService.hashValueRaw(algorithm, KNOWN_VALUE.getBytes());

        // Assert
        assertEquals(explicitUTF8Hash, implicitUTF8Hash);
        assertArrayEquals(explicitUTF8HashBytes, implicitUTF8HashBytes);
        assertArrayEquals(explicitUTF8HashBytes, implicitUTF8HashBytesDefault);
    }

    @Test
    void testShouldRejectNullAlgorithm() {
        // Arrange
        final List<IllegalArgumentException> errors = new ArrayList<>();

        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValue(null, KNOWN_VALUE, StandardCharsets.UTF_8)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValue(null, KNOWN_VALUE)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValueRaw(null, KNOWN_VALUE, StandardCharsets.UTF_8)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValueRaw(null, KNOWN_VALUE)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValueRaw(null, KNOWN_VALUE.getBytes())));

        errors.forEach(error -> assertTrue(error.getMessage().contains("The hash algorithm cannot be null")));
    }

    @Test
    void testShouldRejectNullValue() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256;

        final List<IllegalArgumentException> errors = new ArrayList<>();

        // Act and Assert
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValue(algorithm, null, StandardCharsets.UTF_8)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValue(algorithm, null)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValueRaw(algorithm, null, StandardCharsets.UTF_8)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValueRaw(algorithm, (String) null)));
        errors.add(assertThrows(IllegalArgumentException.class,
                () -> HashService.hashValueRaw(algorithm, (byte[]) null)));

        // Act
        errors.forEach(error -> assertTrue(error.getMessage().contains("The value cannot be null")));
    }

    @Test
    void testShouldHashConstantValue() throws Exception {
        // Arrange
        final List<HashAlgorithm> algorithms = Arrays.asList(HashAlgorithm.values());

        /* These values were generated using command-line tools (openssl dgst -md5, shasum [-a 1 224 256 384 512 512224 512256], rhash --sha3-224, b2sum -l 224)
         * Ex: {@code $ echo -n "apachenifi" | openssl dgst -md5}
         */
        final Map<String, String> EXPECTED_HASHES = new HashMap<>();
        EXPECTED_HASHES.put("md2", "25d261790198fa543b3436b4755ded91");
        EXPECTED_HASHES.put("md5", "a968b5ec1d52449963dcc517789baaaf");
        EXPECTED_HASHES.put("sha_1", "749806dbcab91a695ac85959aca610d84f03c6a7");
        EXPECTED_HASHES.put("sha_224", "4933803881a4ccb9b3453b829263d3e44852765db12958267ad46135");
        EXPECTED_HASHES.put("sha_256", "dc4bd945723b9c234f1be408e8ceb78660b481008b8ab5b71eb2aa3b4f08357a");
        EXPECTED_HASHES.put("sha_384", "a5205271df448e55afc4a553e91a8fea7d60d080d390d1f3484fcb6318abe94174cf3d36ea4eb1a4d5ed7637c99dec0c");
        EXPECTED_HASHES.put("sha_512", "0846ae23e122fbe090e94d45f886aa786acf426f56496e816a64e292b78c1bb7a962dbfd32c5c73bbee432db400970e22fd65498c862da72a305311332c6f302");
        EXPECTED_HASHES.put("sha_512_224", "ecf78a026035528e3097ea7289257d1819d273f60636060fbba43bfb");
        EXPECTED_HASHES.put("sha_512_256", "d90bdd8ad7e19f2d7848a45782d5dbe056a8213a94e03d9a35d6f44dbe7ee6cd");
        EXPECTED_HASHES.put("sha3_224", "2e9d1ea677847dce686ca2444cc4525f114443652fcb55af4c7286cd");
        EXPECTED_HASHES.put("sha3_256", "b1b3cd90a21ef60caba5ec1bf12ffcb833e52a0ae26f0ab7c4f9ccfa9c5c025b");
        EXPECTED_HASHES.put("sha3_384", "ca699a2447032857bf4f7e84fa316264f0c1870f9330031d5d75a0770644353c268b36d0522a3cf62e60f9401aadc37c");
        EXPECTED_HASHES.put("sha3_512", "cb9059d9b7ec4fde4d9710160a694e7ac2a4dd9969dee43d730066ded7b80d3eefdb4cae7622d21f6cfe16092e24f1ad6ca5924767118667654cf71b7abaaca4");
        EXPECTED_HASHES.put("blake2_160", "7bc5a408dba4f1934d9090c4d75c65bfa0c7c90c");
        EXPECTED_HASHES.put("blake2_256", "40b8935dc5ed153846fb08dac8e7999ba04a74f4dab28415c39847a15c211447");
        EXPECTED_HASHES.put("blake2_384", "40716eddc8cfcf666d980804fed294c43fe9436a9787367a3086b45d69791fd5cef1a16c17235ea289c1e40a899b4f6b");
        EXPECTED_HASHES.put("blake2_512", "5f34525b130c11c469302ef6734bf6eedb1eca5d7445a3c4ae289ab58dd13ef72531966bfe2f67c4bf49c99dd14dae92d245f241482307d29bf25c45a1085026");

        // Act
        final Map<String, String> generatedHashes = algorithms
                .stream()
                .collect(Collectors.toMap(HashAlgorithm::getName, algorithm -> HashService.hashValue(algorithm, KNOWN_VALUE, StandardCharsets.UTF_8)));

        // Assert
        for (final Map.Entry<String, String> entry : generatedHashes.entrySet()) {
            final String algorithmName = entry.getKey();
            final String hash = entry.getValue();
            String key = translateStringToMapKey(algorithmName);
            assertEquals(EXPECTED_HASHES.get(key), hash);
        }
    }

    @Test
    void testShouldHashEmptyValue() throws Exception {
        // Arrange
        final List<HashAlgorithm> algorithms = Arrays.asList(HashAlgorithm.values());
        final String EMPTY_VALUE = "";

        /* These values were generated using command-line tools (openssl dgst -md5, shasum [-a 1 224 256 384 512 512224 512256], rhash --sha3-224, b2sum -l 224)
         * Ex: {@code $ echo -n "" | openssl dgst -md5}
         */
        final Map<String, String> EXPECTED_HASHES = new HashMap<>();
        EXPECTED_HASHES.put("md2", "8350e5a3e24c153df2275c9f80692773");
        EXPECTED_HASHES.put("md5", "d41d8cd98f00b204e9800998ecf8427e");
        EXPECTED_HASHES.put("sha_1", "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        EXPECTED_HASHES.put("sha_224", "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f");
        EXPECTED_HASHES.put("sha_256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        EXPECTED_HASHES.put("sha_384", "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b");
        EXPECTED_HASHES.put("sha_512", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
        EXPECTED_HASHES.put("sha_512_224", "6ed0dd02806fa89e25de060c19d3ac86cabb87d6a0ddd05c333b84f4");
        EXPECTED_HASHES.put("sha_512_256", "c672b8d1ef56ed28ab87c3622c5114069bdd3ad7b8f9737498d0c01ecef0967a");
        EXPECTED_HASHES.put("sha3_224", "6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7");
        EXPECTED_HASHES.put("sha3_256", "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a");
        EXPECTED_HASHES.put("sha3_384", "0c63a75b845e4f7d01107d852e4c2485c51a50aaaa94fc61995e71bbee983a2ac3713831264adb47fb6bd1e058d5f004");
        EXPECTED_HASHES.put("sha3_512", "a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26");
        EXPECTED_HASHES.put("blake2_160", "3345524abf6bbe1809449224b5972c41790b6cf2");
        EXPECTED_HASHES.put("blake2_256", "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8");
        EXPECTED_HASHES.put("blake2_384", "b32811423377f52d7862286ee1a72ee540524380fda1724a6f25d7978c6fd3244a6caf0498812673c5e05ef583825100");
        EXPECTED_HASHES.put("blake2_512", "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce");

        // Act
        final Map<String, String> generatedHashes = algorithms
                .stream()
                .collect(Collectors.toMap(HashAlgorithm::getName, algorithm -> HashService.hashValue(algorithm, EMPTY_VALUE, StandardCharsets.UTF_8)));

        // Assert
        for (final Map.Entry<String, String> entry : generatedHashes.entrySet()) {
            final String algorithmName = entry.getKey();
            final String hash = entry.getValue();
            String key = translateStringToMapKey(algorithmName);
            assertEquals(EXPECTED_HASHES.get(key), hash);
        }
    }

    @Test
    void testShouldBuildHashAlgorithmAllowableValues() throws Exception {
        // Arrange
        final List<HashAlgorithm> EXPECTED_ALGORITHMS = Arrays.asList(HashAlgorithm.values());

        // Act
        final AllowableValue[] allowableValues = HashService.buildHashAlgorithmAllowableValues();

        // Assert
        assertInstanceOf(AllowableValue[].class, allowableValues);

        final List<AllowableValue> valuesList = Arrays.asList(allowableValues);
        assertEquals(EXPECTED_ALGORITHMS.size(), valuesList.size());
        EXPECTED_ALGORITHMS.forEach(expectedAlgorithm -> {
            final AllowableValue matchingValue = valuesList
                    .stream()
                    .filter(value -> value.getValue().equals(expectedAlgorithm.getName()))
                    .findFirst()
                    .get();
            assertEquals(expectedAlgorithm.getName(), matchingValue.getDisplayName());
            assertEquals(expectedAlgorithm.buildAllowableValueDescription(), matchingValue.getDescription());
        });
    }

    @Test
    void testShouldBuildCharacterSetAllowableValues() throws Exception {
        // Arrange
        final List<Charset> EXPECTED_CHARACTER_SETS = Arrays.asList(
                StandardCharsets.US_ASCII,
                StandardCharsets.ISO_8859_1,
                StandardCharsets.UTF_8,
                StandardCharsets.UTF_16BE,
                StandardCharsets.UTF_16LE,
                StandardCharsets.UTF_16
        );

        final Map<String, String> expectedDescriptions = Collections.singletonMap(
                "UTF-16",
                "This character set normally decodes using an optional BOM at the beginning of the data but encodes by inserting a BE BOM. For hashing, it will be replaced with UTF-16BE. "
        );

        // Act
        final AllowableValue[] allowableValues = HashService.buildCharacterSetAllowableValues();

        // Assert
        assertInstanceOf(AllowableValue[].class, allowableValues);

        final List<AllowableValue> valuesList = Arrays.asList(allowableValues);

        assertEquals(EXPECTED_CHARACTER_SETS.size(), valuesList.size());

        EXPECTED_CHARACTER_SETS.forEach(charset -> {
            final AllowableValue matchingValue = valuesList
                    .stream()
                    .filter(value -> value.getValue() == charset.name())
                    .findFirst()
                    .get();
            assertEquals(charset.name(), matchingValue.getDisplayName());
            assertEquals((expectedDescriptions.containsKey(charset.name()) ? expectedDescriptions.get(charset.name()) : charset.displayName()), matchingValue.getDescription());
        });
    }

    @Test
    void testShouldHashValueFromStream() throws Exception {
        // Arrange

        // No command-line md2sum tool available
        final List<HashAlgorithm> algorithms = new ArrayList<>(Arrays.asList(HashAlgorithm.values()));
        algorithms.remove(HashAlgorithm.MD2);

        StringBuilder sb = new StringBuilder();
        final int times = 10000;
        for (int i = 0; i < times; i++) {
            sb.append(String.format("%s: %s\n", StringUtils.leftPad(String.valueOf(i), 5), StringUtils.repeat("apachenifi ", 10)));
        }

        /* These values were generated using command-line tools (openssl dgst -md5, shasum [-a 1 224 256 384 512 512224 512256], rhash --sha3-224, b2sum -l 160)
         * Ex: {@code $ openssl dgst -md5 src/test/resources/HashServiceTest/largefile.txt}
         */
        final Map<String, String> EXPECTED_HASHES = new HashMap<>();
        EXPECTED_HASHES.put("md5", "8d329076847b678449610a5fb53997d2");
        EXPECTED_HASHES.put("sha_1", "09cd981ee7529cfd6268a69c0d53e8117e9c78b1");
        EXPECTED_HASHES.put("sha_224", "4d4d58c226959e0775e627a866eaa26bf18121d578b559946aea6f8c");
        EXPECTED_HASHES.put("sha_256", "ce50f183a8011a86c5162e94481c6b14ad921a8001746806063b3033e71440eb");
        EXPECTED_HASHES.put("sha_384", "62a13a410566856422f0b81b2e6ab26f91b3da1a877a5c24f681d2812f26abbc43fb637954879915b3cd9aad626ca71c");
        EXPECTED_HASHES.put("sha_512", "3f036116c78b1d9e2017bb1fd4b04f449839e6434c94442edebffdcdfbac1d79b483978126f0ffb12824f14ecc36a07dc95f0ba04aa68885456f3f6381471e07");
        EXPECTED_HASHES.put("sha_512_224", "aa7227a80889366a2325801a5cfa67f29c8f272f4284aecfe5daba3c");
        EXPECTED_HASHES.put("sha_512_256", "76faa424ee31bcb1f3a41a848806e288cb064a6bf1867881ee1b439dd8b38e40");
        EXPECTED_HASHES.put("sha3_224", "d4bb36bf2d00117ade2e63c6fa2ef5f6714d8b6c7a40d12623f95fd0");
        EXPECTED_HASHES.put("sha3_256", "f93ff4178bc7f466444a822191e152332331ba51eee42b952b3be1b46b1921f7");
        EXPECTED_HASHES.put("sha3_384", "7e4dfb0073645f059e5837f7c066bffd7f8b5d888b0179a8f0be6bb11c7d631847c468d4d861abcdc96503d91f2a7a78");
        EXPECTED_HASHES.put("sha3_512", "bf8e83f3590727e04777406e1d478615cf68468ad8690dba3f22a879e08022864a2b4ad8e8a1cbc88737578abd4b2e8493e3bda39a81af3f21fc529c1a7e3b52");
        EXPECTED_HASHES.put("blake2_160", "71dd4324a1f72aa10aaa59ee4d79ceee8d8915e6");
        EXPECTED_HASHES.put("blake2_256", "5a25864c69f42adeefc343989babb6972df38da47bb6ce712fbef4474266b539");
        EXPECTED_HASHES.put("blake2_384", "52417243317ca01693ba835bd5d6655c73a2f70d811b4d26ddacf9e3b74fc3993f30adc64fb6c23a6a5c1e36771a0b95");
        EXPECTED_HASHES.put("blake2_512", "be81dbc396a9e11c6189d2408a956466fb1c784d2d34495f9ca43434041b425675005deaeea1a04b1f44db0200b19cde5a40fd5e88414bb300620bc3d5e30f6a");

        // Act
        final Map<String, String> generatedHashes = algorithms
                .stream()
                .collect(Collectors.toMap(HashAlgorithm::getName, algorithm -> {
                    // Get a new InputStream for each iteration, or it will calculate the hash of an empty input on iterations 1 - n
                    InputStream input = new ByteArrayInputStream(sb.toString().getBytes());
                    try {
                        return HashService.hashValueStreaming(algorithm, input);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));

        // Assert
        for (final Map.Entry<String, String> entry : generatedHashes.entrySet()) {
            final String algorithmName = entry.getKey();
            final String hash = entry.getValue();
            String key = translateStringToMapKey(algorithmName);
            assertEquals(EXPECTED_HASHES.get(key), hash);
        }
    }

    private static String translateStringToMapKey(String string) {
        return string.toLowerCase().replaceAll("[-\\/]", "_");
    }
}

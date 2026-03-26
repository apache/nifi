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
package org.apache.nifi.services.iceberg.gcs;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import okio.ByteString;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.AUTHORIZATION;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.CONTENT_RANGE;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.LOCATION;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.OAUTH2_TOKEN;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.READ_CHUNK_SIZE_BYTES;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.SERVICE_HOST;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.USER_PROJECT;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.WRITE_CHUNK_SIZE_BYTES;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoogleCloudStorageFileIOTest {

    private static final String TEST_TOKEN = "test-token";
    private static final String TOKEN = "token";
    private static final String BEARER_TEST_TOKEN = "Bearer test-token";

    private static final String DELETE_METHOD = "DELETE";
    private static final String POST_METHOD = "POST";
    private static final String PUT_METHOD = "PUT";

    private static final String UPLOAD_SESSION_PATH = "/upload-session";

    private static final String BUCKET_PATH_FILE_URI = "gs://bucket/path/to/file.parquet";
    private static final String BUCKET_MISSING_FILE_URI = "gs://bucket/missing-file.parquet";
    private static final String BUCKET_EXISTS_URI = "gs://bucket/exists.parquet";
    private static final String BUCKET_MISSING_URI = "gs://bucket/missing.parquet";
    private static final String BUCKET_FILE_PARQUET_URI = "gs://bucket/file.parquet";
    private static final String BUCKET_OUTPUT_URI = "gs://bucket/output.parquet";
    private static final String BUCKET_FILE_TXT_URI = "gs://bucket/file.txt";
    private static final String BUCKET_KEY_URI = "gs://bucket/objectKey";

    private static final String CONTENT_RANGE_FORMAT = "bytes %d-%d/%d";
    private static final String SIZE_RESPONSE_FORMAT = """
            {"size": "%d"}""";
    private static final String SIZE_RESPONSE_12345 = """
            {"size": "12345"}""";
    private static final String USER_PROJECT_QUERY = "userProject=my-project";
    private static final String MY_PROJECT = "my-project";

    private static final String HELLO_WORLD = "hello world";
    private static final String READ_CONTENT = "test file content for reading";
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String READ_CHUNK_SIZE_10 = "10";
    private static final String LOCALHOST_URL_FORMAT = "http://localhost:%d";

    private static final long EXPECTED_LENGTH = 12345L;
    private static final long KNOWN_LENGTH = 42L;

    private MockWebServer mockWebServer;
    private GoogleCloudStorageFileIO fileIO;
    private String baseUrl;

    @BeforeEach
    void setServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        baseUrl = LOCALHOST_URL_FORMAT.formatted(mockWebServer.getPort());
    }

    @AfterEach
    void closeServer() {
        if (fileIO != null) {
            fileIO.close();
        }
        mockWebServer.close();
    }

    @Test
    void testInitializeProperties() {
        fileIO = new GoogleCloudStorageFileIO();
        final Map<String, String> props = Map.of(
                OAUTH2_TOKEN.getProperty(), TEST_TOKEN,
                SERVICE_HOST.getProperty(), baseUrl
        );
        fileIO.initialize(props);

        assertEquals(props, fileIO.properties());
    }

    @Test
    void testDeleteFile() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_NO_CONTENT)
                .build());

        fileIO = createFileIO(TEST_TOKEN);
        fileIO.deleteFile(BUCKET_PATH_FILE_URI);

        final RecordedRequest request = mockWebServer.takeRequest();
        assertEquals(DELETE_METHOD, request.getMethod());
        assertEquals(BEARER_TEST_TOKEN, request.getHeaders().get(AUTHORIZATION.getHeader()));
    }

    @Test
    void testDeleteFileNotFoundIgnored() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_NOT_FOUND)
                .build());

        fileIO = createFileIO(null);
        fileIO.deleteFile(BUCKET_MISSING_FILE_URI);
    }

    @Test
    void testDeleteFileFailedServerError() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_INTERNAL_ERROR)
                .build());

        fileIO = createFileIO(null);
        assertThrows(HttpResponseException.class, () -> fileIO.deleteFile(BUCKET_PATH_FILE_URI));
    }

    @Test
    void testDeleteFileRequestFailed() {
        mockWebServer.close();

        fileIO = createFileIO(null);
        assertThrows(HttpRequestException.class, () -> fileIO.deleteFile(BUCKET_PATH_FILE_URI));
    }

    @Test
    void testInputFileExists() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .build());

        fileIO = createFileIO(TOKEN);
        assertTrue(fileIO.newInputFile(BUCKET_EXISTS_URI).exists());
    }

    @Test
    void testInputFileNotExists() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_NOT_FOUND)
                .build());

        fileIO = createFileIO(null);
        assertFalse(fileIO.newInputFile(BUCKET_MISSING_URI).exists());
    }

    @Test
    void testInputFileGetLength() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .body(SIZE_RESPONSE_12345)
                .build());

        fileIO = createFileIO(TOKEN);
        assertEquals(EXPECTED_LENGTH, fileIO.newInputFile(BUCKET_FILE_PARQUET_URI).getLength());
    }

    @Test
    void testInputFileKnownLength() {
        fileIO = createFileIO(null);

        final InputFile input = fileIO.newInputFile(BUCKET_FILE_PARQUET_URI, KNOWN_LENGTH);
        assertEquals(KNOWN_LENGTH, input.getLength());
    }

    @Test
    void testOutputFileCreateAndWrite() throws IOException, InterruptedException {
        final String sessionUrl = baseUrl + UPLOAD_SESSION_PATH;

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .addHeader(LOCATION.getHeader(), sessionUrl)
                .build());

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .build());

        fileIO = createFileIO(TOKEN, Map.of(
                WRITE_CHUNK_SIZE_BYTES.getProperty(), String.valueOf(GoogleCloudStorageProperties.MINIMUM_CHUNK_SIZE)
        ));

        final OutputFile outputFile = fileIO.newOutputFile(BUCKET_OUTPUT_URI);
        assertNotNull(outputFile.location());

        final byte[] data = HELLO_WORLD.getBytes(StandardCharsets.UTF_8);
        try (PositionOutputStream out = outputFile.createOrOverwrite()) {
            out.write(data);
            assertEquals(data.length, out.getPos());
        }

        final RecordedRequest initRequest = mockWebServer.takeRequest();
        assertEquals(POST_METHOD, initRequest.getMethod());

        final RecordedRequest uploadRequest = mockWebServer.takeRequest();
        assertEquals(PUT_METHOD, uploadRequest.getMethod());

        final ByteString requestBody = uploadRequest.getBody();
        assertNotNull(requestBody);
        assertArrayEquals(data, requestBody.toByteArray());
    }

    @Test
    void testReadClose() throws IOException {
        final byte[] content = READ_CONTENT.getBytes(StandardCharsets.UTF_8);

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .body(SIZE_RESPONSE_FORMAT.formatted(content.length))
                .build());

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_PARTIAL)
                .addHeader(CONTENT_RANGE.getHeader(), CONTENT_RANGE_FORMAT.formatted(0, content.length - 1, content.length))
                .body(READ_CONTENT)
                .build());

        fileIO = createFileIO(TOKEN);

        final InputFile inputFile = fileIO.newInputFile(BUCKET_FILE_TXT_URI);
        assertEquals(content.length, inputFile.getLength());

        try (SeekableInputStream stream = inputFile.newStream()) {
            final byte[] result = new byte[content.length];
            int totalRead = 0;
            while (totalRead < content.length) {
                final int n = stream.read(result, totalRead, content.length - totalRead);
                if (n < 0) {
                    break;
                }
                totalRead += n;
            }
            assertEquals(content.length, totalRead);
            assertArrayEquals(content, result);
            assertEquals(content.length, stream.getPos());
        }
    }

    @Test
    void testSeekableInputStreamSeek() throws IOException {
        final byte[] content = ALPHABET.getBytes(StandardCharsets.UTF_8);

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_PARTIAL)
                .addHeader(CONTENT_RANGE.getHeader(), CONTENT_RANGE_FORMAT.formatted(10, 19, content.length))
                .body(ALPHABET.substring(10, 20))
                .build());

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_PARTIAL)
                .addHeader(CONTENT_RANGE.getHeader(), CONTENT_RANGE_FORMAT.formatted(25, 25, content.length))
                .body(ALPHABET.substring(25, 26))
                .build());

        fileIO = createFileIO(TOKEN, Map.of(
                READ_CHUNK_SIZE_BYTES.getProperty(), READ_CHUNK_SIZE_10
        ));

        try (SeekableInputStream stream = fileIO.newInputFile(BUCKET_FILE_TXT_URI, content.length).newStream()) {
            assertEquals(0, stream.getPos());

            stream.seek(10);
            assertEquals(10, stream.getPos());
            assertEquals('K', stream.read());
            assertEquals(11, stream.getPos());

            stream.seek(25);
            assertEquals('Z', stream.read());
            assertEquals(-1, stream.read());
        }
    }

    @Test
    void testUserProjectQueryParam() {
        final GoogleCloudStorageProperties storageProperties = new GoogleCloudStorageProperties(Map.of(
                SERVICE_HOST.getProperty(), baseUrl,
                USER_PROJECT.getProperty(), MY_PROJECT
        ));

        final GoogleCloudStorageLocation loc = GoogleCloudStorageLocation.parse(BUCKET_KEY_URI);
        assertTrue(storageProperties.metadataUri(loc).contains(USER_PROJECT_QUERY));
        assertTrue(storageProperties.uploadUri(loc).contains(USER_PROJECT_QUERY));
        assertTrue(storageProperties.downloadUri(loc).contains(USER_PROJECT_QUERY));
    }

    private GoogleCloudStorageFileIO createFileIO(final String token) {
        return createFileIO(token, Map.of());
    }

    private GoogleCloudStorageFileIO createFileIO(final String token, final Map<String, String> extraProps) {
        final GoogleCloudStorageFileIO io = new GoogleCloudStorageFileIO();
        final Map<String, String> props = new HashMap<>();
        props.put(SERVICE_HOST.getProperty(), baseUrl);
        if (token != null) {
            props.put(OAUTH2_TOKEN.getProperty(), token);
        }
        props.putAll(extraProps);
        io.initialize(props);
        return io;
    }
}

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
package org.apache.nifi.processors.aws.s3.util;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.RequestPayer;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class S3UtilTest {

    @Test
    void testNullIfBlankWithNotBlank() {
        String value = "value";
        assertEquals(value, S3Util.nullIfBlank(value));
    }

    @Test
    void testNullIfBlankWithBlank() {
        assertNull(S3Util.nullIfBlank(" "));
    }

    @Test
    void testNullIfBlankWithEmpty() {
        assertNull(S3Util.nullIfBlank(""));
    }

    @Test
    void testNullIfBlankWithNull() {
        assertNull(S3Util.nullIfBlank(null));
    }


    @Test
    void testRequestPayerWithFalse() {
        assertNull(S3Util.getRequestPayer(false));
    }

    @Test
    void testRequestPayerWithTrue() {
        assertEquals(RequestPayer.REQUESTER, S3Util.getRequestPayer(true));
    }


    @Test
    void testGetResourceURL() {
        String bucket = "myBucket";
        String key = "myKey";
        Region region = Region.US_WEST_2;
        S3Client s3Client = S3Client.builder().region(region).build();
        String url = String.format("https://s3.%s.amazonaws.com/%s/%s", region.id(), bucket, key);

        assertEquals(url, S3Util.getResourceUrl(s3Client, bucket, key));
    }


    @Test
    void testSanitizeETagWithQuotes() {
        String eTag = "ETAG";
        String eTagWithQuotes = String.format("\"%s\"", eTag);
        assertEquals(eTag, S3Util.sanitizeETag(eTagWithQuotes));
    }

    @Test
    void testSanitizeETagWithoutQuotes() {
        String eTag = "ETAG";
        assertEquals(eTag, S3Util.sanitizeETag(eTag));
    }

    @Test
    void testSanitizeETagWithNull() {
        assertNull(S3Util.sanitizeETag(null));
    }


    @Test
    void testParseExpirationHeaderWithValidHeader() {
        ZonedDateTime expiryDate = ZonedDateTime.of(2025, 10, 22, 20, 00, 0, 0, ZoneId.of("UTC"));
        String ruleId = "myRuleId";
        String header = String.format("expiry-date=\"%s\", rule-id=\"%s\"", expiryDate.format(DateTimeFormatter.RFC_1123_DATE_TIME), ruleId);

        Expiration expiration = S3Util.parseExpirationHeader(header);

        assertEquals(expiryDate.toInstant(), expiration.expirationTime());
        assertEquals(ruleId, expiration.expirationTimeRuleId());
    }

    @Test
    void testParseExpirationHeaderWithInvalidHeader() {
        String header = "no-store, no-cache, must-revalidate";
        assertNull(S3Util.parseExpirationHeader(header));
    }

    @Test
    void testParseExpirationHeaderWithNull() {
        assertNull(S3Util.parseExpirationHeader(null));
    }


    @Test
    void testCreateSpecWithStartOnly() {
        assertEquals("bytes=500-", S3Util.createRangeSpec(500));
    }

    @Test
    void testCreateRangeSpecWithStartAndEnd() {
        assertEquals("bytes=500-1000", S3Util.createRangeSpec(500, 1000));
    }
}

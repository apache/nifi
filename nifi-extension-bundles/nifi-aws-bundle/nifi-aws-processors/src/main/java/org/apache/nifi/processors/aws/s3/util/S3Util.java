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

import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class S3Util {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3Util.class);

    private static final Pattern EXPIRATION_HEADER_PATTERN = Pattern.compile("expiry-date=\"(.*)\", rule-id=\"(.*)\"");

    private S3Util() {
    }

    public static String nullIfBlank(final String value) {
        return StringUtils.isNotBlank(value) ? value : null;
    }

    /**
     * Converts requesterPays boolean flag into RequestPayer enum value or null.
     *
     * @param requesterPays boolean flag indicating whether the requester pays
     * @return RequestPayer.REQUESTER if requesterPays is true, null otherwise
     */
    public static RequestPayer getRequestPayer(final boolean requesterPays) {
        return requesterPays ? RequestPayer.REQUESTER : null;
    }

    /**
     * Mimics AWS SDK v1 AmazonS3Client.getResourceUrl(bucket, key)
     *
     * @param client an already configured S3Client (with region, endpoint, etc.)
     * @param bucket the name of the S3 bucket
     * @param key    the object key
     * @return the full URL as a String
     */
    public static String getResourceUrl(final S3Client client, final String bucket, final String key) {
        return client.utilities()
                .getUrl(GetUrlRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build())
                .toExternalForm();
    }

    /**
     * Strips enclosing quotes from the raw eTag value returned by the AWS service.
     *
     * @param eTag the raw eTag value
     * @return the sanitized eTag value without quotes
     */
    public static String sanitizeETag(final String eTag) {
        if (StringUtils.isNotBlank(eTag)) {
            return eTag.replaceAll("^\"|\"$", "");
        } else {
            return eTag;
        }
    }

    /**
     * Parses the raw x-amz-expiration HTTP header returned by the AWS service into an Expiration object.
     *
     * @param expirationHeader the raw expiration value
     * @return Expiration object containing the expiration time and rule id if parsing was successful, null otherwise
     */
    public static Expiration parseExpirationHeader(final String expirationHeader) {
        Expiration expiration = null;

        if (StringUtils.isNotBlank(expirationHeader)) {
            try {
                final Matcher matcher = EXPIRATION_HEADER_PATTERN.matcher(expirationHeader);
                if (matcher.matches()) {
                    final Instant expirationTime = ZonedDateTime.parse(matcher.group(1), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
                    final String expirationTimeRuleId = URLDecoder.decode(matcher.group(2), StandardCharsets.UTF_8);
                    expiration = new Expiration(expirationTime, expirationTimeRuleId);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to parse expiration header [{}]", expirationHeader, e);
            }
        }

        return expiration;
    }

    /**
     * Converts the start range boundary into Range HTTP header format.
     *
     * @param rangeStart the start position of the range (inclusive)
     * @return the range header string
     */
    public static String createRangeSpec(long rangeStart) {
        return String.format("bytes=%d-", rangeStart);
    }

    /**
     * Converts the start/end range boundaries into Range HTTP header format.
     *
     * @param rangeStart the start position of the range (inclusive)
     * @param rangeEnd   the end position of the range (inclusive)
     * @return the range header string
     */
    public static String createRangeSpec(long rangeStart, long rangeEnd) {
        return String.format("bytes=%d-%d", rangeStart, rangeEnd);
    }

}

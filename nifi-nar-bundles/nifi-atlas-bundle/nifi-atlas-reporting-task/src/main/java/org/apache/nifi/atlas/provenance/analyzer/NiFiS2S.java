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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.resolver.ClusterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class NiFiS2S extends AbstractNiFiProvenanceEventAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiS2S.class);

    private static final Pattern RAW_URL_REGEX = Pattern.compile("([0-9a-zA-Z\\-]+)");
    private static final Pattern HTTP_URL_REGEX = Pattern.compile(".*/nifi-api/data-transfer/(in|out)put-ports/([[0-9a-zA-Z\\-]]+)/transactions/.*");

    protected S2STransitUrl parseTransitURL(String transitUri, ClusterResolver clusterResolver) {
        final URL url = parseUrl(transitUri);

        final String clusterName = clusterResolver.fromHostNames(url.getHost());
        final String targetPortId;
        final String protocol = url.getProtocol().toLowerCase();
        switch (protocol) {

            case "http":
            case "https": {
                final Matcher uriMatcher = matchUrl(url, HTTP_URL_REGEX);
                targetPortId = uriMatcher.group(2);
            }
            break;

            case "nifi": {
                final Matcher uriMatcher = matchUrl(url, RAW_URL_REGEX);
                targetPortId = uriMatcher.group(1);
            }
            break;

            default:
                throw new IllegalArgumentException("Protocol " + protocol + " is not supported as NiFi S2S transit URL.");

        }

        return new S2STransitUrl(clusterName, targetPortId);

    }

    private Matcher matchUrl(URL url, Pattern pattern) {
        final Matcher uriMatcher = pattern.matcher(url.getPath());
        if (!uriMatcher.matches()) {
            throw new IllegalArgumentException("Unexpected transit URI: " + url);
        }
        return uriMatcher;
    }

    protected static class S2STransitUrl {
        final String clusterName;
        final String targetPortId;

        public S2STransitUrl(String clusterName, String targetPortId) {
            this.clusterName = clusterName;
            this.targetPortId = targetPortId;
        }
    }

}

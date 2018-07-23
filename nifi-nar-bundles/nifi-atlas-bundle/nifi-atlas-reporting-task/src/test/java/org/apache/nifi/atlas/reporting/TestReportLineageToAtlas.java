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
package org.apache.nifi.atlas.reporting;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_NIFI_URL;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_PASSWORD;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_URLS;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_USER;
import static org.junit.Assert.assertTrue;

public class TestReportLineageToAtlas {

    private final Logger logger = LoggerFactory.getLogger(TestReportLineageToAtlas.class);

    @Test
    public void validateAtlasUrls() throws Exception {
        final ReportLineageToAtlas reportingTask = new ReportLineageToAtlas();
        final MockProcessContext processContext = new MockProcessContext(reportingTask);
        final MockValidationContext validationContext = new MockValidationContext(processContext);

        processContext.setProperty(ATLAS_NIFI_URL, "http://nifi.example.com:8080/nifi");
        processContext.setProperty(ATLAS_USER, "admin");
        processContext.setProperty(ATLAS_PASSWORD, "admin");

        BiConsumer<Collection<ValidationResult>, Consumer<ValidationResult>> assertResults = (rs, a) -> {
            assertTrue(rs.iterator().hasNext());
            for (ValidationResult r : rs) {
                logger.info("{}", r);
                final String subject = r.getSubject();
                if (ATLAS_URLS.getDisplayName().equals(subject)) {
                    a.accept(r);
                }
            }
        };

        // Default setting.
        assertResults.accept(reportingTask.validate(validationContext),
                r -> assertTrue("Atlas URLs is required", !r.isValid()));


        // Invalid URL.
        processContext.setProperty(ATLAS_URLS, "invalid");
        assertResults.accept(reportingTask.validate(validationContext),
                r -> assertTrue("Atlas URLs is invalid", !r.isValid()));

        // Valid URL
        processContext.setProperty(ATLAS_URLS, "http://atlas.example.com:21000");
        assertTrue(processContext.isValid());

        // Valid URL with Expression
        processContext.setProperty(ATLAS_URLS, "http://atlas.example.com:${literal(21000)}");
        assertTrue(processContext.isValid());

        // Valid URLs
        processContext.setProperty(ATLAS_URLS, "http://atlas1.example.com:21000, http://atlas2.example.com:21000");
        assertTrue(processContext.isValid());

        // Invalid and Valid URLs
        processContext.setProperty(ATLAS_URLS, "invalid, http://atlas2.example.com:21000");
        assertResults.accept(reportingTask.validate(validationContext),
                r -> assertTrue("Atlas URLs is invalid", !r.isValid()));
    }

}

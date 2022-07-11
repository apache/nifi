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
package org.apache.nifi.flow.resource;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractHttpsExternalResourceProviderTest {
    private static final Comparator<ExternalResourceDescriptor> DESCRIPTOR_COMPARATOR =
            Comparator.comparing(ExternalResourceDescriptor::getLocation)
                    .thenComparing(ExternalResourceDescriptor::getPath)
                    .thenComparing(ExternalResourceDescriptor::getLastModified)
                    .thenComparing(ExternalResourceDescriptor::isDirectory);

    protected void assertSuccess(final Collection<ExternalResourceDescriptor> expected, final Collection<ExternalResourceDescriptor> actual) {
        assertEquals(expected.size(), actual.size());

        Set<String> missingDescriptors = new HashSet<>();
        for (ExternalResourceDescriptor desc1 : actual) {
            boolean found = false;
            for (ExternalResourceDescriptor desc2 : expected) {
                if (DESCRIPTOR_COMPARATOR.compare(desc1, desc2) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missingDescriptors.add(desc1.getLocation());
            }
        }

        assertTrue(missingDescriptors.isEmpty(), "Unexpected elemet(s): " + StringUtils.join(missingDescriptors, " ,"));
    }

    protected void assertResourceIsNotIncluded(final Collection<ExternalResourceDescriptor> actual, final String location) {
        assertFalse(actual.stream()
                .map(ExternalResourceDescriptor::getLocation)
                .collect(Collectors.toSet())
                .contains(location));
    }

    protected String normalizeURL(final String url) {
        return url.replaceAll("(?<!http:|https:)/+/", "/");
    }

    protected static class HtmlBuilder {
        private String html = "";

        public static HtmlBuilder newInstance() {
            return new HtmlBuilder();
        }

        private HtmlBuilder() {}

        public HtmlBuilder htmlStart() {
            html = html + "<html>" + System.lineSeparator();
            return this;
        }

        public HtmlBuilder htmlEnd() {
            html = html + "</html>";
            return this;
        }

        public HtmlBuilder tableStart() {
            html = html + "<table>" + System.lineSeparator();
            return this;
        }

        public HtmlBuilder tableEnd() {
            html = html + "</table>" + System.lineSeparator();
            return this;
        }

        public HtmlBuilder tableHeaderEntry() {
            html = html + "<tr>" + System.lineSeparator() +
                    "<th>Name</th><th>Last Modified</th><th>Size</th>" + System.lineSeparator() +
                    "</tr>" + System.lineSeparator();
            return this;
        }

        public HtmlBuilder parentEntry() {
            html = html + "<tr>" + System.lineSeparator() +
                    "<td><a href=\"../\">Parent Directory</a></td>" + System.lineSeparator() +
                    "</tr>" + System.lineSeparator();
            return this;
        }

        public HtmlBuilder fileEntry(final String fileName, final String lastModified, final String size) {
            html = html +  "<tr>" + System.lineSeparator() +
                    "<td><a href=\"" + fileName + "\">" + fileName + "</a></td>" + System.lineSeparator() +
                    "<td>" + lastModified + "</td>" + System.lineSeparator() +
                    "<td>" + size + "</td>" + System.lineSeparator() +
                    "</tr>" + System.lineSeparator();
            return this;
        }

        public HtmlBuilder directoryEntry(final String directoryName) {
            html = html +  "<tr>" + System.lineSeparator() +
                    "<td><a href=\"" + directoryName + "/\">" + directoryName + "/</a></td>" + System.lineSeparator() +
                    "<td>-</td>" + System.lineSeparator() +
                    "<td>-</td>" + System.lineSeparator() +
                    "</tr>" + System.lineSeparator();
            return this;
        }

        public String build() {
            return html;
        }
    }
}

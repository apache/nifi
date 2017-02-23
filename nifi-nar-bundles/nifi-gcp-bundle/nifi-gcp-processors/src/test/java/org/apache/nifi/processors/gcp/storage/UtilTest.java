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
package org.apache.nifi.processors.gcp.storage;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests the Util class static methods.
 */
public class UtilTest {
    @Test
    public void testContentDispositionParsing() throws Exception {
        final String contentDisposition = "attachment; filename=\"plans.pdf\"";

        final Util.ParsedContentDisposition parsed = Util.parseContentDisposition(contentDisposition);
        assertNotNull(parsed);
        assertEquals("plans.pdf",
                parsed.getFileName());

        assertEquals("attachment",
                parsed.getContentDispositionType());
    }

    @Test
    public void testContentDispositionParsingBadParse() throws Exception {
        final String contentDisposition = "bad-header";

        assertNull(Util.parseContentDisposition(contentDisposition));
    }
}

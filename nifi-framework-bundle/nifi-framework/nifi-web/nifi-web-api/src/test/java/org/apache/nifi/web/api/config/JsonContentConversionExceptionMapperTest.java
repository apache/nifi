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
package org.apache.nifi.web.api.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.ws.rs.core.Response;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.databind.JsonMappingException.wrapWithPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(MockitoExtension.class)
public class JsonContentConversionExceptionMapperTest {
    @Mock
    private JsonParser mockParser;

    private JsonContentConversionExceptionMapper jsonCCEM;

    @BeforeEach
    public void setUp() {
        jsonCCEM = new JsonContentConversionExceptionMapper();
    }

    @Test
    public void testShouldThrowExceptionWithStringPortValue() {
        try(Response response = jsonCCEM.toResponse(buildInvalidFormatException("thisIsAnInvalidPort"))) {
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
            assertEquals("The provided proxyPort value 'thisIsAnInvalidPort' is not" +
                    " of required type class java.lang.Integer", response.getEntity());
        }
    }

    @Test
    public void testShouldSanitizeScriptInInput() {
        try(Response response = jsonCCEM.toResponse(buildInvalidFormatException("<script>alert(1);</script>"))) {
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
            assertFalse(Pattern.compile("<script.*>").matcher(response.getEntity().toString()).find());
        }
    }

    private InvalidFormatException buildInvalidFormatException(String value) {
        InvalidFormatException ife = InvalidFormatException.from(mockParser, "Some message", value, Integer.class);
        wrapWithPath(ife, new JsonMappingException.Reference("RemoteProcessGroupDTO", "proxyPort"));
        wrapWithPath(ife, new JsonMappingException.Reference("RemoteProcessGroupEntity", "component"));

        return ife;
    }
}

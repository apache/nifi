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
package org.apache.nifi.parameter;

import org.apache.nifi.registry.flow.mapping.SensitiveValueEncryptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
class TestStandardParameterValueMapper {
    private static final String NAME = "NamedParameter";

    private static final String VALUE = "ParameterValue";

    @Mock
    private SensitiveValueEncryptor sensitiveValueEncryptor;

    private StandardParameterValueMapper mapper;

    @BeforeEach
    void setMapper() {
        mapper = new StandardParameterValueMapper(sensitiveValueEncryptor);
    }

    @Test
    void testGetMappedNotSensitiveNotProvided() {
        final Parameter parameter = getParameter(false, false);

        final String mapped = mapper.getMapped(parameter, VALUE);

        assertEquals(VALUE, mapped);
    }

    @Test
    void testGetMappedNotSensitiveProvided() {
        final Parameter parameter = getParameter(false, true);

        final String mapped = mapper.getMapped(parameter, VALUE);

        assertEquals(StandardParameterValueMapper.PROVIDED_MAPPING, mapped);
    }

    @Test
    void testGetMappedSensitiveProvided() {
        final Parameter parameter = getParameter(true, true);

        final String mapped = mapper.getMapped(parameter, VALUE);

        assertEquals(StandardParameterValueMapper.PROVIDED_MAPPING, mapped);
    }

    @Test
    void testGetMappedSensitiveNotProvided() {
        final Parameter parameter = getParameter(true, false);

        final String mapped = mapper.getMapped(parameter, VALUE);

        assertNotEquals(VALUE, mapped);
        assertNotEquals(StandardParameterValueMapper.PROVIDED_MAPPING, mapped);
    }

    @Test
    void testGetMappedSensitiveNotProvidedNullValue() {
        final Parameter parameter = getParameter(true, false);

        final String mapped = mapper.getMapped(parameter, null);

        assertNull(mapped);
    }

    private Parameter getParameter(final boolean sensitive, final boolean provided) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(NAME).sensitive(sensitive).build();
        return new Parameter.Builder().descriptor(descriptor).value(VALUE).provided(provided).build();
    }
}

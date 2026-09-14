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

import org.apache.nifi.security.encryption.PropertyEncryptionEncoder;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.SensitivePropertyAttribute;
import org.apache.nifi.security.encryption.SensitivePropertyCategory;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandardParameterValueMapper {
    private static final String NAME = "NamedParameter";

    private static final String VALUE = "ParameterValue";

    private static final String CONTEXT_NAME = "NamedParameterContext";

    private static final byte[] PROVIDER_VALUE = {1, 2, 3, 4};

    private static final String PROVIDER_MAPPED_VALUE = PropertyEncryptionEncoder.getEncoded("01020304");

    @Mock
    private PropertyEncryptionProvider propertyEncryptionProvider;

    @Captor
    private ArgumentCaptor<SensitivePropertyContext> contextCaptor;

    @Captor
    private ArgumentCaptor<byte[]> propertyCaptor;

    private StandardParameterValueMapper mapper;

    @BeforeEach
    void setMapper() {
        mapper = new StandardParameterValueMapper(propertyEncryptionProvider);
    }

    @Test
    void testGetMappedNotSensitiveNotProvided() {
        final Parameter parameter = getParameter(false, false);

        final String mapped = mapper.getMapped(CONTEXT_NAME, parameter, VALUE);

        assertEquals(VALUE, mapped);
    }

    @Test
    void testGetMappedNotSensitiveProvided() {
        final Parameter parameter = getParameter(false, true);

        final String mapped = mapper.getMapped(CONTEXT_NAME, parameter, VALUE);

        assertEquals(StandardParameterValueMapper.PROVIDED_MAPPING, mapped);
    }

    @Test
    void testGetMappedSensitiveProvided() {
        final Parameter parameter = getParameter(true, true);

        final String mapped = mapper.getMapped(CONTEXT_NAME, parameter, VALUE);

        assertEquals(StandardParameterValueMapper.PROVIDED_MAPPING, mapped);
    }

    @Test
    void testGetMappedSensitiveNotProvidedNullValue() {
        final Parameter parameter = getParameter(true, false);

        final String mapped = mapper.getMapped(CONTEXT_NAME, parameter, null);

        assertNull(mapped);
    }

    @Test
    void testGetMappedPropertyEncryptionProvider() {
        final Parameter parameter = getParameter(true, false);
        when(propertyEncryptionProvider.encrypt(any(), any())).thenReturn(PROVIDER_VALUE);

        final String mapped = mapper.getMapped(CONTEXT_NAME, parameter, VALUE);

        assertEquals(PROVIDER_MAPPED_VALUE, mapped);

        verify(propertyEncryptionProvider).encrypt(propertyCaptor.capture(), contextCaptor.capture());
        assertArrayEquals(VALUE.getBytes(StandardCharsets.UTF_8), propertyCaptor.getValue());

        final SensitivePropertyContext context = contextCaptor.getValue();
        assertEquals(SensitivePropertyCategory.PARAMETER, context.category());

        final Map<String, String> attributes = context.attributes();
        assertEquals(CONTEXT_NAME, attributes.get(SensitivePropertyAttribute.PARAMETER_CONTEXT_NAME.getKey()));
        assertEquals(NAME, attributes.get(SensitivePropertyAttribute.PARAMETER_NAME.getKey()));
    }

    private Parameter getParameter(final boolean sensitive, final boolean provided) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(NAME).sensitive(sensitive).build();
        return new Parameter.Builder().descriptor(descriptor).value(VALUE).provided(provided).build();
    }
}

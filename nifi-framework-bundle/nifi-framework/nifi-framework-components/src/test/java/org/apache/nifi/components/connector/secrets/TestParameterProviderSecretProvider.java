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
package org.apache.nifi.components.connector.secrets;

import org.apache.nifi.components.connector.PropertyProtectionType;
import org.apache.nifi.components.connector.Secret;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.ParameterTag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestParameterProviderSecretProvider {

    private static final String TAG_NAME = "nifi.unrestricted";

    @Test
    public void testTagValueTrueLowercaseIsUnrestricted() {
        assertEquals(PropertyProtectionType.UNRESTRICTED, classify(TAG_NAME, List.of(new ParameterTag(TAG_NAME, "true"))));
    }

    @Test
    public void testTagValueTrueTitlecaseIsUnrestricted() {
        assertEquals(PropertyProtectionType.UNRESTRICTED, classify(TAG_NAME, List.of(new ParameterTag(TAG_NAME, "True"))));
    }

    @Test
    public void testTagValueTrueUppercaseIsUnrestricted() {
        assertEquals(PropertyProtectionType.UNRESTRICTED, classify(TAG_NAME, List.of(new ParameterTag(TAG_NAME, "TRUE"))));
    }

    @Test
    public void testTagValueFalseIsRestricted() {
        assertEquals(PropertyProtectionType.RESTRICTED, classify(TAG_NAME, List.of(new ParameterTag(TAG_NAME, "false"))));
    }

    @Test
    public void testTagValueNullIsRestricted() {
        assertEquals(PropertyProtectionType.RESTRICTED, classify(TAG_NAME, List.of(new ParameterTag(TAG_NAME, null))));
    }

    @Test
    public void testTagKeyMismatchIsRestricted() {
        assertEquals(PropertyProtectionType.RESTRICTED, classify(TAG_NAME, List.of(new ParameterTag("other-tag", "true"))));
    }

    @Test
    public void testNoTagsIsRestricted() {
        assertEquals(PropertyProtectionType.RESTRICTED, classify(TAG_NAME, List.of()));
    }

    @Test
    public void testConfiguredNameNullIsAlwaysRestricted() {
        assertEquals(PropertyProtectionType.RESTRICTED, classify(null, List.of(new ParameterTag(TAG_NAME, "true"))));
    }

    @Test
    public void testConfiguredNameBlankIsAlwaysRestricted() {
        assertEquals(PropertyProtectionType.RESTRICTED, classify("   ", List.of(new ParameterTag(TAG_NAME, "true"))));
    }

    @Test
    public void testMultipleTagsOneMatchIsUnrestricted() {
        assertEquals(PropertyProtectionType.UNRESTRICTED, classify(TAG_NAME,
            List.of(new ParameterTag("other-tag", "false"), new ParameterTag(TAG_NAME, "true"))));
    }

    private PropertyProtectionType classify(final String configuredTagName, final List<ParameterTag> tags) {
        final ParameterProviderNode node = mock(ParameterProviderNode.class);
        when(node.getIdentifier()).thenReturn("provider-id");
        when(node.getName()).thenReturn("Provider");

        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
            .name("secret")
            .description("description")
            .build();
        final Parameter parameter = new Parameter.Builder()
            .descriptor(descriptor)
            .value("secret-value")
            .tags(tags)
            .build();
        when(node.fetchParameterValues()).thenReturn(List.of(new ParameterGroup("Group", List.of(parameter))));

        final ParameterProviderSecretProvider provider = new ParameterProviderSecretProvider(node, configuredTagName);
        final List<Secret> secrets = provider.getAllSecrets();
        assertEquals(1, secrets.size());
        return secrets.get(0).getPropertyProtectionType();
    }
}

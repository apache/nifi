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

package org.apache.nifi.components.connector;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardConnectorConfigurationContext {

    private static final String PROVIDER_1_ID = "provider-1";
    private static final String PROVIDER_1_NAME = "Provider1";
    private static final String PROVIDER_2_ID = "provider-2";
    private static final String PROVIDER_2_NAME = "Provider2";

    private static final String SECRET_VALUE_1 = "resolved-value-1";
    private static final String SECRET_VALUE_2 = "resolved-value-2";
    private static final String SECRET_VALUE_3 = "resolved-value-3";
    private static final String PLAIN_VALUE = "plainValue";
    private static final String ASSET_PATH = "/path/to/asset";

    private static final SecretReference SECRET_REF_1 = new SecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, "secret1", "Provider1.group.secret1");
    private static final SecretReference SECRET_REF_2 = new SecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, "secret2", "Provider1.group.secret2");
    private static final SecretReference SECRET_REF_3 = new SecretReference(PROVIDER_2_ID, PROVIDER_2_NAME, "secret3", "Provider2.group.secret3");

    private StandardConnectorConfigurationContext context;

    @BeforeEach
    public void setUp() {
        final AssetManager assetManager = mock(AssetManager.class);
        final SecretsManager secretsManager = mock(SecretsManager.class);
        context = new StandardConnectorConfigurationContext(assetManager, secretsManager);
    }

    private StepConfiguration toStepConfiguration(final Map<String, String> stringProperties) {
        final Map<String, ConnectorValueReference> valueReferences = new HashMap<>();
        for (final Map.Entry<String, String> entry : stringProperties.entrySet()) {
            final String value = entry.getValue();
            valueReferences.put(entry.getKey(), value == null ? null : new StringLiteralValue(value));
        }
        return new StepConfiguration(valueReferences);
    }

    private static Secret mockSecret(final String value) {
        final Secret secret = mock(Secret.class);
        when(secret.getValue()).thenReturn(value);
        return secret;
    }

    @Test
    public void testSetPropertiesWithNoExistingConfigurations() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        context.setProperties("step1", toStepConfiguration(properties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("value2", context.getProperty("step1", "key2").getValue());
    }

    @Test
    public void testSetPropertiesAddsNewProperties() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        final Map<String, String> newProperties = new HashMap<>();
        newProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(newProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesMergesExistingPropertiesWithNewValues() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", "updatedValue2");
        updatedProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(updatedProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("updatedValue2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesRemovesKeyWhenValueIsNull() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        initialProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        assertEquals("value2", context.getProperty("step1", "key2").getValue());

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", null);
        context.setProperties("step1", toStepConfiguration(updatedProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertNull(context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesLeavesUnprovidedKeysAsIs() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        initialProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", "updatedValue2");
        context.setProperties("step1", toStepConfiguration(updatedProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("updatedValue2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesForDifferentSteps() {
        final Map<String, String> step1Properties = new HashMap<>();
        step1Properties.put("key1", "value1");
        context.setProperties("step1", toStepConfiguration(step1Properties));

        final Map<String, String> step2Properties = new HashMap<>();
        step2Properties.put("key2", "value2");
        context.setProperties("step2", toStepConfiguration(step2Properties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertNull(context.getProperty("step1", "key2").getValue());
        assertEquals("value2", context.getProperty("step2", "key2").getValue());
        assertNull(context.getProperty("step2", "key1").getValue());
    }

    @Test
    public void testGetPropertyReturnsEmptyForNonExistentStep() {
        final ConnectorPropertyValue propertyValue = context.getProperty("nonExistentStep", "someProperty");
        assertNull(propertyValue.getValue());
    }

    @Test
    public void testGetPropertyReturnsEmptyForNonExistentProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        context.setProperties("step1", toStepConfiguration(properties));

        final ConnectorPropertyValue propertyValue = context.getProperty("step1", "nonExistentProperty");
        assertNull(propertyValue.getValue());
    }

    @Test
    public void testComplexMergingScenario() {
        final Map<String, String> initialProps = new HashMap<>();
        initialProps.put("a", "1");
        initialProps.put("b", "2");
        initialProps.put("c", "3");
        initialProps.put("d", "4");
        initialProps.put("e", "5");
        initialProps.put("f", "6");
        context.setProperties("step1", toStepConfiguration(initialProps));

        final Map<String, String> updateProps = new HashMap<>();
        updateProps.put("b", null);
        updateProps.put("c", "30");
        updateProps.put("g", "7");
        updateProps.put("h", "8");
        context.setProperties("step1", toStepConfiguration(updateProps));

        assertEquals("1", context.getProperty("step1", "a").getValue());
        assertNull(context.getProperty("step1", "b").getValue());
        assertEquals("30", context.getProperty("step1", "c").getValue());
        assertEquals("4", context.getProperty("step1", "d").getValue());
        assertEquals("5", context.getProperty("step1", "e").getValue());
        assertEquals("6", context.getProperty("step1", "f").getValue());
        assertEquals("7", context.getProperty("step1", "g").getValue());
        assertEquals("8", context.getProperty("step1", "h").getValue());
    }

    @Test
    public void testCloneHasCorrectResolvedValues() {
        final Map<String, String> step1Properties = new HashMap<>();
        step1Properties.put("key1", "value1");
        step1Properties.put("key2", "value2");
        step1Properties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(step1Properties));

        final Map<String, String> step2Properties = new HashMap<>();
        step2Properties.put("keyA", "valueA");
        step2Properties.put("keyB", "valueB");
        context.setProperties("step2", toStepConfiguration(step2Properties));

        final MutableConnectorConfigurationContext clonedContext = context.clone();

        assertEquals("value1", clonedContext.getProperty("step1", "key1").getValue());
        assertEquals("value2", clonedContext.getProperty("step1", "key2").getValue());
        assertEquals("value3", clonedContext.getProperty("step1", "key3").getValue());
        assertEquals("valueA", clonedContext.getProperty("step2", "keyA").getValue());
        assertEquals("valueB", clonedContext.getProperty("step2", "keyB").getValue());
        assertNull(clonedContext.getProperty("step1", "nonExistent").getValue());
        assertNull(clonedContext.getProperty("nonExistentStep", "key1").getValue());
    }

    @Test
    public void testResolvePropertyValuesResolvesSecretsThatWereInitiallyUnresolvable() {
        final Secret secret = mockSecret(SECRET_VALUE_1);

        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getSecrets(anySet()))
            .thenReturn(Map.of())
            .thenReturn(Map.of(SECRET_REF_1, secret));

        final StandardConnectorConfigurationContext testContext = new StandardConnectorConfigurationContext(mock(AssetManager.class), secretsManager);

        final Map<String, ConnectorValueReference> properties = new HashMap<>();
        properties.put("plain", new StringLiteralValue(PLAIN_VALUE));
        properties.put("secret", SECRET_REF_1);
        testContext.setProperties("step1", new StepConfiguration(properties));

        assertEquals(PLAIN_VALUE, testContext.getProperty("step1", "plain").getValue());
        assertNull(testContext.getProperty("step1", "secret").getValue());

        testContext.resolvePropertyValues();

        assertEquals(PLAIN_VALUE, testContext.getProperty("step1", "plain").getValue());
        assertEquals(SECRET_VALUE_1, testContext.getProperty("step1", "secret").getValue());
    }

    @Test
    public void testSetPropertiesBatchesSecretResolution() {
        final Secret secret1 = mockSecret(SECRET_VALUE_1);
        final Secret secret2 = mockSecret(SECRET_VALUE_2);

        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getSecrets(anySet())).thenReturn(Map.of(SECRET_REF_1, secret1, SECRET_REF_2, secret2));

        final StandardConnectorConfigurationContext testContext = new StandardConnectorConfigurationContext(mock(AssetManager.class), secretsManager);

        final Map<String, ConnectorValueReference> properties = new HashMap<>();
        properties.put("plain", new StringLiteralValue(PLAIN_VALUE));
        properties.put("secret1", SECRET_REF_1);
        properties.put("secret2", SECRET_REF_2);
        testContext.setProperties("step1", new StepConfiguration(properties));

        assertEquals(PLAIN_VALUE, testContext.getProperty("step1", "plain").getValue());
        assertEquals(SECRET_VALUE_1, testContext.getProperty("step1", "secret1").getValue());
        assertEquals(SECRET_VALUE_2, testContext.getProperty("step1", "secret2").getValue());
        verify(secretsManager, times(1)).getSecrets(anySet());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testResolvePropertyValuesBatchesAcrossAllSteps() {
        final Secret secret1 = mockSecret(SECRET_VALUE_1);
        final Secret secret3 = mockSecret(SECRET_VALUE_3);

        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getSecrets(anySet())).thenReturn(Map.of(SECRET_REF_1, secret1, SECRET_REF_3, secret3));

        final StandardConnectorConfigurationContext testContext = new StandardConnectorConfigurationContext(mock(AssetManager.class), secretsManager);

        final Map<String, ConnectorValueReference> step1Props = new HashMap<>();
        step1Props.put("secret1", SECRET_REF_1);
        testContext.setProperties("step1", new StepConfiguration(step1Props));

        final Map<String, ConnectorValueReference> step2Props = new HashMap<>();
        step2Props.put("secret3", SECRET_REF_3);
        testContext.setProperties("step2", new StepConfiguration(step2Props));

        verify(secretsManager, times(2)).getSecrets(anySet());

        testContext.resolvePropertyValues();

        final ArgumentCaptor<Set<SecretReference>> captor = ArgumentCaptor.forClass(Set.class);
        verify(secretsManager, times(3)).getSecrets(captor.capture());
        assertEquals(Set.of(SECRET_REF_1, SECRET_REF_3), captor.getAllValues().get(2));

        assertEquals(SECRET_VALUE_1, testContext.getProperty("step1", "secret1").getValue());
        assertEquals(SECRET_VALUE_3, testContext.getProperty("step2", "secret3").getValue());
    }

    @Test
    public void testMixedPropertyTypesResolvedCorrectly() {
        final AssetReference assetRef = new AssetReference(Set.of("asset-1"));

        final Secret secret = mockSecret(SECRET_VALUE_1);

        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getSecrets(anySet())).thenReturn(Map.of(SECRET_REF_1, secret));

        final Asset asset = mock(Asset.class);
        final File assetFile = mock(File.class);
        when(assetFile.getAbsolutePath()).thenReturn(ASSET_PATH);
        when(asset.getFile()).thenReturn(assetFile);

        final AssetManager assetManager = mock(AssetManager.class);
        when(assetManager.getAsset("asset-1")).thenReturn(Optional.of(asset));

        final StandardConnectorConfigurationContext testContext = new StandardConnectorConfigurationContext(assetManager, secretsManager);

        final Map<String, ConnectorValueReference> properties = new HashMap<>();
        properties.put("plain", new StringLiteralValue(PLAIN_VALUE));
        properties.put("secret", SECRET_REF_1);
        properties.put("asset", assetRef);
        testContext.setProperties("step1", new StepConfiguration(properties));

        assertEquals(PLAIN_VALUE, testContext.getProperty("step1", "plain").getValue());
        assertEquals(SECRET_VALUE_1, testContext.getProperty("step1", "secret").getValue());
        assertEquals(ASSET_PATH, testContext.getProperty("step1", "asset").getValue());
    }

    @Test
    public void testUnresolvableSecretReferenceMapsToNull() {
        final Map<SecretReference, Secret> resultWithNull = new HashMap<>();
        resultWithNull.put(SECRET_REF_1, null);

        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getSecrets(anySet())).thenReturn(resultWithNull);

        final StandardConnectorConfigurationContext testContext = new StandardConnectorConfigurationContext(mock(AssetManager.class), secretsManager);

        final Map<String, ConnectorValueReference> properties = new HashMap<>();
        properties.put("secret", SECRET_REF_1);
        testContext.setProperties("step1", new StepConfiguration(properties));

        assertNull(testContext.getProperty("step1", "secret").getValue());
    }

    @Test
    public void testNoSecretReferencesDoesNotCallGetSecrets() {
        final SecretsManager secretsManager = mock(SecretsManager.class);
        final StandardConnectorConfigurationContext testContext = new StandardConnectorConfigurationContext(mock(AssetManager.class), secretsManager);

        final Map<String, ConnectorValueReference> properties = new HashMap<>();
        properties.put("plain1", new StringLiteralValue("value1"));
        properties.put("plain2", new StringLiteralValue("value2"));
        testContext.setProperties("step1", new StepConfiguration(properties));

        assertEquals("value1", testContext.getProperty("step1", "plain1").getValue());
        assertEquals("value2", testContext.getProperty("step1", "plain2").getValue());
        verify(secretsManager, never()).getSecrets(anySet());
    }

    @Test
    public void testReplacePropertiesBatchesSecretResolution() {
        final Secret secret1 = mockSecret(SECRET_VALUE_1);
        final Secret secret2 = mockSecret(SECRET_VALUE_2);

        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getSecrets(anySet())).thenReturn(Map.of(SECRET_REF_1, secret1, SECRET_REF_2, secret2));

        final StandardConnectorConfigurationContext testContext = new StandardConnectorConfigurationContext(mock(AssetManager.class), secretsManager);

        final Map<String, ConnectorValueReference> properties = new HashMap<>();
        properties.put("secret1", SECRET_REF_1);
        properties.put("secret2", SECRET_REF_2);
        testContext.replaceProperties("step1", new StepConfiguration(properties));

        assertEquals(SECRET_VALUE_1, testContext.getProperty("step1", "secret1").getValue());
        assertEquals(SECRET_VALUE_2, testContext.getProperty("step1", "secret2").getValue());
        verify(secretsManager, times(1)).getSecrets(anySet());
    }
}

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

package org.apache.nifi.controller;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.validation.EnablingServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class TestAbstractComponentNode {

    private static final String PROPERTY_NAME = "abstract-property-name";

    private static final String PROPERTY_VALUE = "abstract-property-value";

    @Timeout(5)
    @Test
    public void testGetValidationStatusWithTimeout() {
        final ValidationControlledAbstractComponentNode node = new ValidationControlledAbstractComponentNode(5000, mock(ValidationTrigger.class));
        final ValidationStatus status = node.getValidationStatus(1, TimeUnit.MILLISECONDS);
        assertEquals(ValidationStatus.VALIDATING, status);
    }

    @Test
    public void testOnParametersModified() {
        final AtomicLong validationCount = new AtomicLong(0L);
        final ValidationTrigger validationTrigger = new ValidationTrigger() {
            @Override
            public void triggerAsync(ComponentNode component) {
                validationCount.incrementAndGet();
            }

            @Override
            public void trigger(ComponentNode component) {
                validationCount.incrementAndGet();
            }
        };

        final List<PropertyModification> propertyModifications = new ArrayList<>();
        final ValidationControlledAbstractComponentNode node = new ValidationControlledAbstractComponentNode(0, validationTrigger) {
            @Override
            protected void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
                propertyModifications.add(new PropertyModification(descriptor, oldValue, newValue));
                super.onPropertyModified(descriptor, oldValue, newValue);
            }
        };

        final ParameterContext context = mock(ParameterContext.class);
        final ParameterDescriptor paramDescriptor = new ParameterDescriptor.Builder()
            .name("abc")
            .description("")
            .sensitive(false)
            .build();
        final Parameter param = new Parameter.Builder().descriptor(paramDescriptor).value("123").build();
        Mockito.doReturn(Optional.of(param)).when(context).getParameter("abc");
        node.setParameterContext(context);

        final Map<String, String> properties = new HashMap<>();
        properties.put("abc", "#{abc}");
        node.setProperties(properties, false, Collections.emptySet());

        assertEquals(1, propertyModifications.size());
        PropertyModification mod = propertyModifications.get(0);
        assertNull(mod.getPreviousValue());
        assertEquals("123", mod.getUpdatedValue());
        propertyModifications.clear();

        final Map<String, ParameterUpdate> updatedParameters = new HashMap<>();
        updatedParameters.put("abc", new MockParameterUpdate("abc", "old-value", "123", false));
        node.onParametersModified(updatedParameters);

        assertEquals(1, propertyModifications.size());
        mod = propertyModifications.get(0);
        assertEquals("old-value", mod.getPreviousValue());
        assertEquals("123", mod.getUpdatedValue());
    }

    @Test
    public void testMismatchedSensitiveFlags() {
        final LocalComponentNode node = new LocalComponentNode();

        final ParameterContext context = mock(ParameterContext.class);
        final ParameterDescriptor paramDescriptor = new ParameterDescriptor.Builder()
            .name("abc")
            .description("")
            .sensitive(true)
            .build();
        final Parameter param = new Parameter.Builder().descriptor(paramDescriptor).value("123").build();
        Mockito.doReturn(Optional.of(param)).when(context).getParameter("abc");
        node.setParameterContext(context);

        final String propertyValue = "#{abc}";
        final PropertyDescriptor propertyDescriptor = new PropertyDescriptor.Builder()
            .name("abc")
            .sensitive(false)
            .dynamic(true)
            .addValidator(Validator.VALID)
            .build();

        final Map<String, String> properties = new HashMap<>();
        properties.put("abc", propertyValue);
        node.verifyCanUpdateProperties(properties);
        node.setProperties(properties, false, Collections.emptySet());

        final ValidationContext validationContext = mock(ValidationContext.class);
        when(validationContext.getProperties()).thenReturn(Collections.singletonMap(propertyDescriptor, propertyValue));
        when(validationContext.getAllProperties()).thenReturn(properties);
        when(validationContext.isDependencySatisfied(any(PropertyDescriptor.class), any(Function.class))).thenReturn(true);
        when(validationContext.getReferencedParameters(Mockito.anyString())).thenReturn(Collections.singleton("abc"));
        when(validationContext.isParameterDefined("abc")).thenReturn(true);

        final ValidationState validationState = node.performValidation(validationContext);
        assertSame(ValidationStatus.INVALID, validationState.getStatus());

        final Collection<ValidationResult> results = validationState.getValidationErrors();
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertFalse(result.isValid());
        assertTrue(result.getExplanation().toLowerCase().contains("sensitivity"));
    }

    @Timeout(10)
    @Test
    public void testValidationTriggerPaused() throws InterruptedException {
        final AtomicLong validationCount = new AtomicLong(0L);

        final ValidationControlledAbstractComponentNode node = new ValidationControlledAbstractComponentNode(0, new ValidationTrigger() {
            @Override
            public void triggerAsync(ComponentNode component) {
                validationCount.incrementAndGet();
            }

            @Override
            public void trigger(ComponentNode component) {
                validationCount.incrementAndGet();
            }
        });

        node.pauseValidationTrigger();
        for (int i = 0; i < 1000; i++) {
            node.setProperties(Collections.emptyMap(), false, Collections.emptySet());
            assertEquals(0, validationCount.get());
        }
        node.resumeValidationTrigger();

        // wait for validation count to be 1 (this is asynchronous so we want to just keep checking).
        while (validationCount.get() != 1) {
            Thread.sleep(50L);
        }

        assertEquals(1L, validationCount.get());
    }

    @Test
    public void testValidateControllerServicesValid() {
        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);
        final ValidationContext context = getServiceValidationContext(ControllerServiceState.ENABLED, serviceProvider);

        final ValidationControlledAbstractComponentNode componentNode = new ValidationControlledAbstractComponentNode(0, mock(ValidationTrigger.class), serviceProvider);
        final Collection<ValidationResult> results = componentNode.validateReferencedControllerServices(context);
        assertTrue(results.isEmpty(), String.format("Validation Failed %s", results));
    }

    @Test
    public void testUpdateProcessorControllerServiceReference() {
        final String controllerServiceIdA = "controllerServiceIdA";
        final ControllerService controllerServiceA = mock(ControllerService.class);
        when(controllerServiceA.getIdentifier()).thenReturn(controllerServiceIdA);
        final ControllerServiceNode controllerServiceNodeA = mock(ControllerServiceNode.class);

        final String controllerServiceIdB = "controllerServiceIdB";
        final ControllerService controllerServiceB = mock(ControllerService.class);
        when(controllerServiceB.getIdentifier()).thenReturn(controllerServiceIdB);
        final ControllerServiceNode controllerServiceNodeB = mock(ControllerServiceNode.class);

        final String propertyName = "record-reader";
        final ConfigurableComponent processor = mock(ConfigurableComponent.class);
        final PropertyDescriptor propertyDescriptorReader = new PropertyDescriptor.Builder()
                .name(propertyName)
                .identifiesControllerService(ControllerService.class)
                .build();
        when(processor.getPropertyDescriptor(eq(propertyName))).thenReturn(propertyDescriptorReader);

        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);
        when(serviceProvider.getControllerService(eq(controllerServiceIdA))).thenReturn(controllerServiceA);
        when(serviceProvider.getControllerServiceNode(eq(controllerServiceIdA))).thenReturn(controllerServiceNodeA);
        when(serviceProvider.getControllerService(eq(controllerServiceIdB))).thenReturn(controllerServiceB);
        when(serviceProvider.getControllerServiceNode(eq(controllerServiceIdB))).thenReturn(controllerServiceNodeB);

        final AbstractComponentNode componentNode = mock(AbstractComponentNode.class, withSettings()
                .defaultAnswer(Answers.CALLS_REAL_METHODS)
                .useConstructor(
                        "id", mock(ValidationContextFactory.class),
                        serviceProvider, "componentType", "componentClass",
                        mock(ReloadComponent.class), mock(ExtensionManager.class),
                        mock(ValidationTrigger.class), false));
        when(componentNode.getControllerServiceProvider()).thenReturn(serviceProvider);
        when(componentNode.getComponent()).thenReturn(processor);

        final Map<String, String> properties = new HashMap<>();
        properties.put("record-reader", controllerServiceIdA);
        componentNode.setProperties(properties);
        verify(controllerServiceNodeA).addReference(eq(componentNode), eq(propertyDescriptorReader));
        verifyNoMoreInteractions(controllerServiceNodeA);

        properties.put("record-reader", controllerServiceIdB);
        componentNode.setProperties(properties);
        verify(controllerServiceNodeA).removeReference(eq(componentNode), eq(propertyDescriptorReader));
        verifyNoMoreInteractions(controllerServiceNodeA);
        verify(controllerServiceNodeB).addReference(eq(componentNode), eq(propertyDescriptorReader));
        verifyNoMoreInteractions(controllerServiceNodeB);
    }

    @Test
    public void testValidateControllerServicesEnablingInvalid() {
        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);
        final ValidationContext context = getServiceValidationContext(ControllerServiceState.ENABLING, serviceProvider);

        final ValidationControlledAbstractComponentNode componentNode = new ValidationControlledAbstractComponentNode(0, mock(ValidationTrigger.class), serviceProvider);
        final Collection<ValidationResult> results = componentNode.validateReferencedControllerServices(context);

        final Optional<ValidationResult> firstResult = results.stream().findFirst();
        assertTrue(firstResult.isPresent(), "Validation Result not found");
        final ValidationResult validationResult = firstResult.get();
        assertInstanceOf(EnablingServiceValidationResult.class, validationResult);
    }

    @Test
    public void testSetProperties() {
        final AbstractComponentNode node = new LocalComponentNode();

        final PropertyDescriptor originalPropertyDescriptor = node.getPropertyDescriptor(PROPERTY_NAME);
        assertTrue(originalPropertyDescriptor.isDynamic());
        assertFalse(originalPropertyDescriptor.isSensitive());

        final Map<String, String> properties = Collections.singletonMap(PROPERTY_NAME, PROPERTY_VALUE);
        node.setProperties(properties);

        final PropertyDescriptor updatedPropertyDescriptor = node.getPropertyDescriptor(PROPERTY_NAME);
        assertTrue(updatedPropertyDescriptor.isDynamic());
        assertFalse(updatedPropertyDescriptor.isSensitive());
    }

    @Test
    public void testSetPropertiesPropertyModified() {
        final String propertyValueModified = PROPERTY_VALUE + "-modified";
        final List<PropertyModification> propertyModifications = new ArrayList<>();
        final AbstractComponentNode node = new LocalComponentNode() {
            @Override
            protected void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
                propertyModifications.add(new PropertyModification(descriptor, oldValue, newValue));
                super.onPropertyModified(descriptor, oldValue, newValue);
            }
        };

        final Map<String, String> properties = new HashMap<>();
        properties.put(PROPERTY_NAME, PROPERTY_VALUE);
        node.setProperties(properties);

        assertEquals(1, propertyModifications.size());
        PropertyModification mod = propertyModifications.get(0);
        assertNull(mod.getPreviousValue());
        assertEquals(PROPERTY_VALUE, mod.getUpdatedValue());
        propertyModifications.clear();

        properties.put(PROPERTY_NAME, propertyValueModified);
        node.setProperties(properties);

        assertEquals(1, propertyModifications.size());
        mod = propertyModifications.get(0);
        assertEquals(PROPERTY_VALUE, mod.getPreviousValue());
        assertEquals(propertyValueModified, mod.getUpdatedValue());
    }

    @Test
    public void testSetPropertiesSensitiveDynamicPropertyNames() {
        final AbstractComponentNode node = new LocalComponentNode();

        final Map<String, String> properties = Collections.singletonMap(PROPERTY_NAME, PROPERTY_VALUE);
        final Set<String> sensitiveDynamicPropertyNames = Collections.singleton(PROPERTY_NAME);
        node.setProperties(properties, false, sensitiveDynamicPropertyNames);

        final PropertyDescriptor updatedPropertyDescriptor = node.getPropertyDescriptor(PROPERTY_NAME);
        assertTrue(updatedPropertyDescriptor.isDynamic());
        assertTrue(updatedPropertyDescriptor.isSensitive());

        final Map<PropertyDescriptor, PropertyConfiguration> configuredProperties = node.getProperties();
        final PropertyDescriptor configuredPropertyDescriptor = configuredProperties.keySet()
                .stream()
                .filter(descriptor -> descriptor.getName().equals(PROPERTY_NAME))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Property Name not found"));
        assertTrue(configuredPropertyDescriptor.isDynamic());
        assertTrue(configuredPropertyDescriptor.isSensitive());

        final PropertyConfiguration propertyConfiguration = configuredProperties.get(configuredPropertyDescriptor);
        assertEquals(PROPERTY_VALUE, propertyConfiguration.getRawValue());
    }

    @Test
    public void testSetPropertiesSensitiveDynamicPropertyNamesAddedRemoved() {
        final AbstractComponentNode node = new LocalComponentNode();

        final Map<String, String> properties = Collections.singletonMap(PROPERTY_NAME, PROPERTY_VALUE);
        final Set<String> sensitiveDynamicPropertyNames = Collections.singleton(PROPERTY_NAME);
        node.setProperties(properties, false, sensitiveDynamicPropertyNames);

        final PropertyDescriptor sensitivePropertyDescriptor = node.getPropertyDescriptor(PROPERTY_NAME);
        assertTrue(sensitivePropertyDescriptor.isDynamic());
        assertTrue(sensitivePropertyDescriptor.isSensitive());

        // Set Properties without updating Sensitive Dynamic Property Names
        node.setProperties(properties, false, Collections.emptySet());

        final PropertyDescriptor updatedPropertyDescriptor = node.getPropertyDescriptor(PROPERTY_NAME);
        assertTrue(updatedPropertyDescriptor.isDynamic());
        assertTrue(updatedPropertyDescriptor.isSensitive());

        final Map<PropertyDescriptor, PropertyConfiguration> configuredProperties = node.getProperties();
        final PropertyDescriptor configuredPropertyDescriptor = configuredProperties.keySet()
                .stream()
                .filter(descriptor -> descriptor.getName().equals(PROPERTY_NAME))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Property Name not found"));
        assertTrue(configuredPropertyDescriptor.isDynamic());
        assertFalse(configuredPropertyDescriptor.isSensitive());

        // Set Properties with value removed to update Sensitive Dynamic Property Names
        final Map<String, String> removedProperties = Collections.singletonMap(PROPERTY_NAME, null);
        node.setProperties(removedProperties, false, Collections.emptySet());

        final PropertyDescriptor removedPropertyDescriptor = node.getPropertyDescriptor(PROPERTY_NAME);
        assertTrue(removedPropertyDescriptor.isDynamic());
        assertFalse(removedPropertyDescriptor.isSensitive());
    }

    private ValidationContext getServiceValidationContext(final ControllerServiceState serviceState, final ControllerServiceProvider serviceProvider) {
        final ValidationContext context = mock(ValidationContext.class);

        final String serviceIdentifier = MockControllerService.class.getName();
        final ControllerServiceNode serviceNode = mock(ControllerServiceNode.class);
        when(serviceProvider.getControllerServiceNode(serviceIdentifier)).thenReturn(serviceNode);
        when(serviceNode.getState()).thenReturn(serviceState);
        when(serviceNode.isActive()).thenReturn(true);

        final PropertyDescriptor property = new PropertyDescriptor.Builder()
                .name(MockControllerService.class.getSimpleName())
                .identifiesControllerService(ControllerService.class)
                .required(true)
                .build();
        final Map<PropertyDescriptor, String> properties = Collections.singletonMap(property, serviceIdentifier);

        when(context.getProperties()).thenReturn(properties);
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn(serviceIdentifier);
        when(context.getProperty(Mockito.eq(property))).thenReturn(propertyValue);
        when(context.isDependencySatisfied(any(PropertyDescriptor.class), any(Function.class))).thenReturn(true);
        return context;
    }

    private static class LocalComponentNode extends AbstractComponentNode {
        private volatile ParameterContext paramContext = null;

        public LocalComponentNode() {
            this(mock(ControllerServiceProvider.class), mock(ValidationTrigger.class));
        }

        public LocalComponentNode(final ControllerServiceProvider controllerServiceProvider, final ValidationTrigger validationTrigger) {
            super("id", mock(ValidationContextFactory.class), controllerServiceProvider, "unit test component",
                ValidationControlledAbstractComponentNode.class.getCanonicalName(), mock(ReloadComponent.class),
                mock(ExtensionManager.class), validationTrigger, false);
        }

        @Override
        public void reload(Set<URL> additionalUrls) throws Exception {
        }

        @Override
        public BundleCoordinate getBundleCoordinate() {
            return null;
        }

        @Override
        public ConfigurableComponent getComponent() {
            final ConfigurableComponent component = mock(ConfigurableComponent.class);
            when(component.getPropertyDescriptor(Mockito.anyString())).thenAnswer(invocation -> {
                final String propertyName = invocation.getArgument(0, String.class);
                return new PropertyDescriptor.Builder()
                    .name(propertyName)
                    .addValidator(Validator.VALID)
                    .dynamic(true)
                    .build();
            });

            return component;
        }

        @Override
        public TerminationAwareLogger getLogger() {
            return null;
        }

        @Override
        public Class<?> getComponentClass() {
            return ValidationControlledAbstractComponentNode.class;
        }

        @Override
        public boolean isRestricted() {
            return false;
        }

        @Override
        public boolean isDeprecated() {
            return false;
        }

        @Override
        public boolean isValidationNecessary() {
            return true;
        }

        @Override
        public Optional<ProcessGroup> getParentProcessGroup() {
            return Optional.empty();
        }

        @Override
        public ParameterLookup getParameterLookup() {
            return ParameterLookup.EMPTY;
        }

        @Override
        public String getProcessGroupIdentifier() {
            return "1234";
        }

        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return null;
        }

        @Override
        protected List<ValidationResult> validateConfig() {
            return Collections.emptyList();
        }

        @Override
        public void verifyModifiable() throws IllegalStateException {
        }

        @Override
        protected ParameterContext getParameterContext() {
            return paramContext;
        }

        protected void setParameterContext(final ParameterContext parameterContext) {
            this.paramContext = parameterContext;
        }
    }


    private static class ValidationControlledAbstractComponentNode extends LocalComponentNode {
        private final long pauseMillis;

        public ValidationControlledAbstractComponentNode(final long pauseMillis, final ValidationTrigger validationTrigger) {
            this(pauseMillis, validationTrigger, mock(ControllerServiceProvider.class));
        }

        public ValidationControlledAbstractComponentNode(final long pauseMillis, final ValidationTrigger validationTrigger, final ControllerServiceProvider controllerServiceProvider) {
            super(controllerServiceProvider, validationTrigger);
            this.pauseMillis = pauseMillis;
        }

        @Override
        protected Collection<ValidationResult> computeValidationErrors(ValidationContext context) {
            try {
                Thread.sleep(pauseMillis);
            } catch (final InterruptedException ignored) {
            }

            return null;
        }
    }

    private static class PropertyModification {
        private final PropertyDescriptor propertyDescriptor;
        private final String previousValue;
        private final String updatedValue;

        public PropertyModification(final PropertyDescriptor propertyDescriptor, final String previousValue, final String updatedValue) {
            this.propertyDescriptor = propertyDescriptor;
            this.previousValue = previousValue;
            this.updatedValue = updatedValue;
        }

        public PropertyDescriptor getPropertyDescriptor() {
            return propertyDescriptor;
        }

        public String getPreviousValue() {
            return previousValue;
        }

        public String getUpdatedValue() {
            return updatedValue;
        }
    }

    private static class MockParameterUpdate implements ParameterUpdate {
        private final String parameterName;
        private final String oldValue;
        private final String newValue;
        private final boolean sensitive;

        public MockParameterUpdate(final String parameterName, final String oldValue, final String newValue, final boolean sensitive) {
            this.parameterName = parameterName;
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.sensitive = sensitive;
        }

        @Override
        public String getParameterName() {
            return parameterName;
        }

        @Override
        public String getPreviousValue() {
            return oldValue;
        }

        @Override
        public String getUpdatedValue() {
            return newValue;
        }

        @Override
        public boolean isSensitive() {
            return sensitive;
        }
    }

    private interface MockControllerService extends ControllerService {

    }
}

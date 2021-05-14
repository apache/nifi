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
import org.apache.nifi.components.validation.EnablingServiceValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterUpdate;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.junit.Test;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAbstractComponentNode {

    @Test(timeout = 5000)
    public void testGetValidationStatusWithTimeout() {
        final ValidationControlledAbstractComponentNode node = new ValidationControlledAbstractComponentNode(5000, Mockito.mock(ValidationTrigger.class));
        final ValidationStatus status = node.getValidationStatus(1, TimeUnit.MILLISECONDS);
        assertEquals(ValidationStatus.VALIDATING, status);
    }

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

        final Map<String, String> properties = new HashMap<>();
        properties.put("abc", "#{abc}");
        node.setProperties(properties);

        final ParameterContext context = Mockito.mock(ParameterContext.class);
        final ParameterDescriptor paramDescriptor = new ParameterDescriptor.Builder()
            .name("abc")
            .description("")
            .sensitive(false)
            .build();
        final Parameter param = new Parameter(paramDescriptor, "123");
        Mockito.doReturn(param).when(context).getParameter("abc");

        final Map<String, ParameterUpdate> updatedParameters = new HashMap<>();
        updatedParameters.put("abc", new MockParameterUpdate("abc", "xyz", "123", false));
        node.onParametersModified(updatedParameters);

        assertEquals(1, propertyModifications.size());
        final PropertyModification mod = propertyModifications.get(0);
        assertEquals("xyz", mod.getPreviousValue());
        assertEquals("123", mod.getUpdatedValue());
    }

    @Test(timeout = 10000)
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
            node.setProperties(Collections.emptyMap());
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
        final ControllerServiceProvider serviceProvider = Mockito.mock(ControllerServiceProvider.class);
        final ValidationContext context = getServiceValidationContext(ControllerServiceState.ENABLED, serviceProvider);

        final ValidationControlledAbstractComponentNode componentNode = new ValidationControlledAbstractComponentNode(0, Mockito.mock(ValidationTrigger.class), serviceProvider);
        final Collection<ValidationResult> results = componentNode.validateReferencedControllerServices(context);
        assertTrue(String.format("Validation Failed %s", results), results.isEmpty());
    }

    @Test
    public void testValidateControllerServicesEnablingInvalid() {
        final ControllerServiceProvider serviceProvider = Mockito.mock(ControllerServiceProvider.class);
        final ValidationContext context = getServiceValidationContext(ControllerServiceState.ENABLING, serviceProvider);

        final ValidationControlledAbstractComponentNode componentNode = new ValidationControlledAbstractComponentNode(0, Mockito.mock(ValidationTrigger.class), serviceProvider);
        final Collection<ValidationResult> results = componentNode.validateReferencedControllerServices(context);

        final Optional<ValidationResult> firstResult = results.stream().findFirst();
        assertTrue("Validation Result not found", firstResult.isPresent());
        final ValidationResult validationResult = firstResult.get();
        assertTrue("Enabling Service Validation Result not found", validationResult instanceof EnablingServiceValidationResult);
    }

    private ValidationContext getServiceValidationContext(final ControllerServiceState serviceState, final ControllerServiceProvider serviceProvider) {
        final ValidationContext context = Mockito.mock(ValidationContext.class);

        final String serviceIdentifier = MockControllerService.class.getName();
        final ControllerServiceNode serviceNode = Mockito.mock(ControllerServiceNode.class);
        Mockito.when(serviceProvider.getControllerServiceNode(serviceIdentifier)).thenReturn(serviceNode);
        Mockito.when(serviceNode.getState()).thenReturn(serviceState);
        Mockito.when(serviceNode.isActive()).thenReturn(true);

        final PropertyDescriptor property = new PropertyDescriptor.Builder()
                .name(MockControllerService.class.getSimpleName())
                .identifiesControllerService(ControllerService.class)
                .required(true)
                .build();
        final Map<PropertyDescriptor, String> properties = Collections.singletonMap(property, serviceIdentifier);

        Mockito.when(context.getProperties()).thenReturn(properties);
        final PropertyValue propertyValue = Mockito.mock(PropertyValue.class);
        Mockito.when(propertyValue.getValue()).thenReturn(serviceIdentifier);
        Mockito.when(context.getProperty(Mockito.eq(property))).thenReturn(propertyValue);
        Mockito.when(context.isDependencySatisfied(Mockito.any(PropertyDescriptor.class), Mockito.any(Function.class))).thenReturn(true);
        return context;
    }

    private static class ValidationControlledAbstractComponentNode extends AbstractComponentNode {
        private final long pauseMillis;
        private volatile ParameterContext paramContext = null;

        public ValidationControlledAbstractComponentNode(final long pauseMillis, final ValidationTrigger validationTrigger) {
            this(pauseMillis, validationTrigger, Mockito.mock(ControllerServiceProvider.class));
        }

        public ValidationControlledAbstractComponentNode(final long pauseMillis, final ValidationTrigger validationTrigger, final ControllerServiceProvider controllerServiceProvider) {
            super("id", Mockito.mock(ValidationContextFactory.class), controllerServiceProvider, "unit test component",
                    ValidationControlledAbstractComponentNode.class.getCanonicalName(), Mockito.mock(ComponentVariableRegistry.class), Mockito.mock(ReloadComponent.class),
                    Mockito.mock(ExtensionManager.class), validationTrigger, false);

            this.pauseMillis = pauseMillis;
        }

        @Override
        protected Collection<ValidationResult> computeValidationErrors(ValidationContext context) {
            try {
                Thread.sleep(pauseMillis);
            } catch (final InterruptedException ie) {
            }

            return null;
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
            return Mockito.mock(ConfigurableComponent.class);
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

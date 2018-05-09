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

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAbstractComponentNode {

    @Test(timeout = 5000)
    public void testGetValidationStatusWithTimeout() {
        final ValidationControlledAbstractComponentNode node = new ValidationControlledAbstractComponentNode();
        final ValidationStatus status = node.getValidationStatus(1, TimeUnit.MILLISECONDS);
        assertEquals(ValidationStatus.VALIDATING, status);
    }


    private static class ValidationControlledAbstractComponentNode extends AbstractComponentNode {

        public ValidationControlledAbstractComponentNode() {
            super("id", Mockito.mock(ValidationContextFactory.class), Mockito.mock(ControllerServiceProvider.class), "unit test component",
                ValidationControlledAbstractComponentNode.class.getCanonicalName(), Mockito.mock(ComponentVariableRegistry.class), Mockito.mock(ReloadComponent.class),
                Mockito.mock(ValidationTrigger.class), false);
        }

        @Override
        protected Collection<ValidationResult> computeValidationErrors(ValidationContext context) {
            try {
                Thread.sleep(5000L);
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
            return null;
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
    }
}

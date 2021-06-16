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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.reporting.InitializationException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestControllerServiceApiMatcher {

    private ControllerServiceApiMatcher serviceApiMatcher;

    @Before
    public void setup() {
        this.serviceApiMatcher = new ControllerServiceApiMatcher();
    }

    @Test
    public void testServiceImplementationMatchesRealServiceApi() {
        assertTrue(serviceApiMatcher.matches(FooServiceApiV1.class, FooServiceImplementationV1.class));
    }

    @Test
    public void testServiceImplementationMatchesCompatibleServiceApi() {
        // v2 implementation should match the real v2 API that it implements
        assertTrue(serviceApiMatcher.matches(FooServiceApiV2.class, FooServiceImplementationV2.class));
        // v2 implementation should also match the v1 API since the methods are the same
        assertTrue(serviceApiMatcher.matches(FooServiceApiV1.class, FooServiceImplementationV2.class));
    }

    @Test
    public void testServiceImplementationDoesNotMatchIncompatibleApi() {
        assertFalse(serviceApiMatcher.matches(FooServiceApiV3.class, FooServiceImplementationV1.class));
        assertFalse(serviceApiMatcher.matches(FooServiceApiV3.class, FooServiceImplementationV2.class));
    }

    @Test
    public void testServiceApiWithGenerics() {
        // should match
        assertTrue(serviceApiMatcher.matches(GenericServiceApiV1.class, GenericServiceImpl.class));
        // should not match because method changed args
        assertFalse(serviceApiMatcher.matches(GenericServiceApiV2.class, GenericServiceImpl.class));
        // should not match because method changed return type
        assertFalse(serviceApiMatcher.matches(GenericServiceApiV3.class, GenericServiceImpl.class));
    }

    // Interface for a result
    private interface FooResult {
        String getResult();
    }

    // Implementation for a result
    private class FooResultImpl implements FooResult {
        private final String result;

        public FooResultImpl(final String result) {
            this.result = result;
        }

        @Override
        public String getResult() {
            return result;
        }
    }

    // Interface for an argument
    private interface FooArg {
        String getArg();
    }

    // Implementation for an argument
    private static class FooArgImpl implements FooArg {
        private final String arg;

        public FooArgImpl(final String arg) {
            this.arg = arg;
        }

        @Override
        public String getArg() {
            return arg;
        }
    }

    // Service API v1
    private interface FooServiceApiV1 extends ControllerService {
        void execute();
        String execute(String a);
        FooResult executeWithReturn(FooArg a);
    }

    // Service API v2, unchanged from v1
    private interface FooServiceApiV2 extends ControllerService {
        void execute();
        String execute(String a);
        FooResult executeWithReturn(FooArg a);
    }

    // Service API v3, new method added since v1/v3
    private interface FooServiceApiV3 extends ControllerService {
        void execute();
        String execute(String a);
        FooResult executeWithReturn(FooArg a);
        String someNewMethod();
    }

    // Service implementing v1 API
    private static class FooServiceImplementationV1 implements FooServiceApiV1 {

        @Override
        public void execute() {

        }

        @Override
        public String execute(String a) {
            return null;
        }

        // Declare the impl result here to test comparing a more specific return type against the API
        @Override
        public FooResultImpl executeWithReturn(FooArg a) {
            return null;
        }

        @Override
        public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

        }

        @Override
        public Collection<ValidationResult> validate(ValidationContext context) {
            return null;
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String name) {
            return null;
        }

        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return null;
        }

        @Override
        public String getIdentifier() {
            return null;
        }
    }

    // Service implementing v2 API
    private static class FooServiceImplementationV2 implements FooServiceApiV2 {

        @Override
        public void execute() {

        }

        @Override
        public String execute(String a) {
            return null;
        }

        @Override
        public FooResult executeWithReturn(FooArg a) {
            return null;
        }

        @Override
        public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

        }

        @Override
        public Collection<ValidationResult> validate(ValidationContext context) {
            return null;
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String name) {
            return null;
        }

        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return null;
        }

        @Override
        public String getIdentifier() {
            return null;
        }
    }

    // Service API with generics v1
    private interface GenericServiceApiV1<T extends FooResult, V extends FooArg> extends ControllerService {
        T execute(V arg);
    }

    // Service API with generics v2
    private interface GenericServiceApiV2<T extends FooResult, V extends FooArg> extends ControllerService {
        T execute(V arg1, V arg2);
    }

    // Service API with generics v3
    private interface GenericServiceApiV3<V extends FooArg> extends ControllerService {
        String execute(V arg);
    }

    // Service implementation with generics
    private static class GenericServiceImpl implements GenericServiceApiV1<FooResultImpl,FooArgImpl> {

        @Override
        public FooResultImpl execute(FooArgImpl arg) {
            return null;
        }

        @Override
        public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

        }

        @Override
        public Collection<ValidationResult> validate(ValidationContext context) {
            return null;
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String name) {
            return null;
        }

        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return null;
        }

        @Override
        public String getIdentifier() {
            return null;
        }
    }

}

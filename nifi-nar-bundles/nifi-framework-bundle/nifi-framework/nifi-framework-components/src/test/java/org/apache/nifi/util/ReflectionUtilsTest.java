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
package org.apache.nifi.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.logging.ComponentLog;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReflectionUtilsTest {

    private List<String> invocations = new ArrayList<>();

    @Before
    public void reset() {
        this.invocations.clear();
    }

    /*
     * Some JDKs will generate Bridge method that will be missing annotation
     * public void org.apache.nifi.util.ReflectionUtilsTest$B.setFoo(java.lang.Object)
     * and will not be invoked. This validates that ReflectionUtils handles it.
     */
    @Test
    public void invokeWithBridgePresent() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new B(), 2);
        assertEquals(2, this.invocations.size());
        assertEquals("B", this.invocations.get(0));
        assertEquals("B", this.invocations.get(1));
    }

    @Test
    public void ensureParentMethodIsCalled() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new C(), 4);
        assertEquals(1, this.invocations.size());
        assertEquals("A", this.invocations.get(0));
    }

    @Test
    public void ensureOnlyOverridenMethodIsCalled() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new D(), "String");
        assertEquals(1, this.invocations.size());
        assertEquals("D", this.invocations.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateFailureWithWrongArgumentType() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new B(), "foo");
    }

    @Test
    public void ensureSuccessWhenArgumentCountDoesntMatch() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new B(), 3, "hjk");
        assertEquals(2, this.invocations.size());
        assertEquals("B", this.invocations.get(0));
        assertEquals("B", this.invocations.get(1));
    }

    @Test
    public void ensureSuccessWithMultipleArgumentsOfPropperType() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new E(), 3, "hjk", "hjk".getBytes());
        assertEquals(1, this.invocations.size());
        assertEquals("E", this.invocations.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateFailureIfOneOfArgumentsWrongType() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new E(), 3, "hjk", "hjk");
    }

    @Test
    public void validateNoFailureIfQuiatelyIfOneOfArgumentsWrongTypeAndProcessLog() throws Exception {
        ComponentLog pl = mock(ComponentLog.class);
        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, new E(), pl, 3, "hjk", "hjk");
        verify(pl, Mockito.atMost(1)).error(Mockito.anyString());
    }

    @Test(expected = InvocationTargetException.class)
    public void validateInvocationFailure() throws Exception {
        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, new F(), 3);
    }

    @Test
    public void validateQuiteSuccessWithInvocationFailure() throws Exception {
        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, new F(), 3);
    }

    public abstract class A<T> {
        @OnStopped
        public void setFoo(T a) {
            invocations.add("A");
        }
    }

    public class B extends A<Integer> {
        @Override
        @OnStopped
        public void setFoo(Integer a) {
            invocations.add("B");
        }
    }

    public class C extends A<String> {

    }

    public class D extends C {
        @Override
        public void setFoo(String a) {
            invocations.add("D");
        }
    }

    public class E {
        @OnStopped
        public void foo(Integer a, String b, byte[] c) {
            invocations.add("E");
        }
    }

    public class F {
        @OnStopped
        public void foo(Integer a) {
            throw new RuntimeException("Intentional");
        }
    }

}

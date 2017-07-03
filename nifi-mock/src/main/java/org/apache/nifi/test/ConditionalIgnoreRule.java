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
package org.apache.nifi.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;

import org.junit.Assume;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public class ConditionalIgnoreRule implements MethodRule {

    public interface IgnoreCondition {
        boolean isSatisfied();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.TYPE})
    public @interface ConditionalIgnore {
        Class<? extends IgnoreCondition> condition();
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        Statement result = base;
        if (hasConditionalIgnoreAnnotation(method)) {
            IgnoreCondition condition = getIgnoreContition(target, method);
            if (condition.isSatisfied()) {
                result = new IgnoreStatement(condition);
            }
        }
        return result;
    }

    private static boolean hasConditionalIgnoreAnnotation(FrameworkMethod method) {
        return method.getAnnotation(ConditionalIgnore.class) != null;
    }

    private static IgnoreCondition getIgnoreContition(Object target, FrameworkMethod method) {
        ConditionalIgnore annotation = method.getAnnotation(ConditionalIgnore.class);
        return new IgnoreConditionCreator(target, annotation).create();
    }

    private static class IgnoreConditionCreator {
        private final Object target;
        private final Class<? extends IgnoreCondition> conditionType;

        IgnoreConditionCreator(Object target, ConditionalIgnore annotation) {
            this.target = target;
            this.conditionType = annotation.condition();
        }

        IgnoreCondition create() {
            checkConditionType();
            try {
                return createCondition();
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private IgnoreCondition createCondition() throws Exception {
            IgnoreCondition result;
            if (isConditionTypeStandalone()) {
                result = conditionType.newInstance();
            } else {
                result = conditionType.getDeclaredConstructor(target.getClass()).newInstance(target);
            }
            return result;
        }

        private void checkConditionType() {
            if (!isConditionTypeStandalone() && !isConditionTypeDeclaredInTarget()) {
                String msg
                = "Conditional class '%s' is a member class "
                        + "but was not declared inside the test case using it.\n"
                        + "Either make this class a static class, "
                        + "standalone class (by declaring it in it's own file) "
                        + "or move it inside the test case using it";
                throw new IllegalArgumentException(String.format(msg, conditionType.getName()));
            }
        }

        private boolean isConditionTypeStandalone() {
            return !conditionType.isMemberClass() || Modifier.isStatic(conditionType.getModifiers());
        }

        private boolean isConditionTypeDeclaredInTarget() {
            return target.getClass().isAssignableFrom(conditionType.getDeclaringClass());
        }
    }

    private static class IgnoreStatement extends Statement {
        private final IgnoreCondition condition;

        IgnoreStatement(IgnoreCondition condition) {
            this.condition = condition;
        }

        @Override
        public void evaluate() {
            Assume.assumeTrue("Ignored by " + condition.getClass().getSimpleName(), false);
        }
    }

}

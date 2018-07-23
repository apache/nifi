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

import java.lang.annotation.Annotation;
import java.util.Arrays;

public class ClassAnnotationPair {
    private final Class<?> clazz;
    private final Class<? extends Annotation>[] annotations;

    public ClassAnnotationPair(final Class<?> clazz, final Class<? extends Annotation>[] annotations) {
        this.clazz = clazz;
        this.annotations = annotations;
    }

    public Class<?> getDeclaredClass() {
        return clazz;
    }

    public Class<? extends Annotation>[] getAnnotations() {
        return annotations;
    }

    @Override
    public int hashCode() {
        return 41 + 47 * clazz.hashCode() + 47 * Arrays.hashCode(annotations);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ClassAnnotationPair)) {
            return false;
        }

        final ClassAnnotationPair other = (ClassAnnotationPair) obj;
        return clazz == other.clazz && Arrays.equals(annotations, other.annotations);
    }
}

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
package org.apache.nifi.web.util;

import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

class ParameterContextNameCollisionResolverTest {
    private final static Collection<ParameterContextEntity> EMPTY_PARAMETER_CONTEXT_SOURCE = Collections.emptySet();
    private final static Collection<ParameterContextEntity> PARAMETER_CONTEXT_SOURCE_WITH_FIRST = Arrays.asList(getTestContext("test"));
    private final static Collection<ParameterContextEntity> PARAMETER_CONTEXT_SOURCE_WITH_SOME =
            Arrays.asList(getTestContext("test"), getTestContext("test (1)"), getTestContext("test (2)"));
    private final static Collection<ParameterContextEntity> PARAMETER_CONTEXT_SOURCE_WITH_NON_CONTINUOUS =
            Arrays.asList(getTestContext("test (3)"), getTestContext("test (9)"));
    private final static Collection<ParameterContextEntity> PARAMETER_CONTEXT_SOURCE_WITH_OTHER_LINEAGES =
            Arrays.asList(getTestContext("test"), getTestContext("test2 (3)"), getTestContext("other"));

    @ParameterizedTest(name = "\"{0}\" into \"{1}\"")
    @MethodSource("testDataSet")
    public void testResolveNameCollision(
            final String oldName,
            final String expectedResult,
            final Collection<ParameterContextEntity> parameterContexts
    ) {
        final ParameterContextNameCollisionResolver testSubject = new ParameterContextNameCollisionResolver();
        final String result = testSubject.resolveNameCollision(oldName, parameterContexts);
        Assertions.assertEquals(expectedResult, result);
    }

    private static Stream<Arguments> testDataSet() {
        return Stream.of(
                Arguments.of("test", "test (1)", EMPTY_PARAMETER_CONTEXT_SOURCE),
                Arguments.of("test (1)", "test (2)", EMPTY_PARAMETER_CONTEXT_SOURCE),
                Arguments.of("test(1)", "test(1) (1)", EMPTY_PARAMETER_CONTEXT_SOURCE),
                Arguments.of("test (1) (1)", "test (1) (2)", EMPTY_PARAMETER_CONTEXT_SOURCE),
                Arguments.of("(1)", "(1) (1)", EMPTY_PARAMETER_CONTEXT_SOURCE),
                Arguments.of(
                        "((((Lorem.ipsum dolor sit.amet, consectetur adipiscing elit",
                        "((((Lorem.ipsum dolor sit.amet, consectetur adipiscing elit (1)",
                        EMPTY_PARAMETER_CONTEXT_SOURCE),
                Arguments.of("test", "test (1)", PARAMETER_CONTEXT_SOURCE_WITH_FIRST),
                Arguments.of("test (1)", "test (2)", PARAMETER_CONTEXT_SOURCE_WITH_FIRST),
                Arguments.of("test (8)", "test (9)", PARAMETER_CONTEXT_SOURCE_WITH_FIRST),
                Arguments.of("other", "other (1)", PARAMETER_CONTEXT_SOURCE_WITH_FIRST),
                Arguments.of("test", "test (3)", PARAMETER_CONTEXT_SOURCE_WITH_SOME),
                Arguments.of("test (1)", "test (3)", PARAMETER_CONTEXT_SOURCE_WITH_SOME),
                Arguments.of("other", "other (1)", PARAMETER_CONTEXT_SOURCE_WITH_SOME),
                Arguments.of("test", "test (10)", PARAMETER_CONTEXT_SOURCE_WITH_NON_CONTINUOUS),
                Arguments.of("test (3)", "test (10)", PARAMETER_CONTEXT_SOURCE_WITH_NON_CONTINUOUS),
                Arguments.of("test (15)", "test (16)", PARAMETER_CONTEXT_SOURCE_WITH_NON_CONTINUOUS),
                Arguments.of("test", "test (1)", PARAMETER_CONTEXT_SOURCE_WITH_OTHER_LINEAGES),
                Arguments.of("test (1)", "test (2)", PARAMETER_CONTEXT_SOURCE_WITH_OTHER_LINEAGES)
        );
    }

    private static ParameterContextEntity getTestContext(final String name) {
        final ParameterContextEntity result = Mockito.mock(ParameterContextEntity.class);
        final ParameterContextDTO dto = Mockito.mock(ParameterContextDTO.class);
        Mockito.when(result.getComponent()).thenReturn(dto);
        Mockito.when(dto.getName()).thenReturn(name);
        return result;
    }
}
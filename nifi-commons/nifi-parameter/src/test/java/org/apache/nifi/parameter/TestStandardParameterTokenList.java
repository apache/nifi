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
package org.apache.nifi.parameter;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class TestStandardParameterTokenList {

    @Test
    public void testSubstitute() {
        final List<ParameterToken> referenceList = new ArrayList<>();
        referenceList.add(new StandardParameterReference("foo", 0, 5, "#{foo}"));

        final ParameterLookup paramLookup = Mockito.mock(ParameterLookup.class);
        Mockito.when(paramLookup.getParameter("foo")).thenReturn(Optional.of(new Parameter(new ParameterDescriptor.Builder().name("foo").build(), "bar")));
        Mockito.when(paramLookup.getParameter("bazz")).thenReturn(Optional.of(new Parameter(new ParameterDescriptor.Builder().name("bazz").build(), "baz")));

        StandardParameterTokenList references = new StandardParameterTokenList("#{foo}", referenceList);
        assertEquals("bar", references.substitute(paramLookup));

        referenceList.add(new StandardParameterReference("bazz", 6, 12, "#{bazz}"));

        references = new StandardParameterTokenList("#{foo}#{bazz}", referenceList);
        assertEquals("barbaz", references.substitute(paramLookup));

        references = new StandardParameterTokenList("#{foo}#{bazz}Hello, World!", referenceList);
        assertEquals("barbazHello, World!", references.substitute(paramLookup));

        referenceList.clear();
        referenceList.add(new StandardParameterReference("foo", 0, 5, "#{foo}"));
    }

    @Test
    public void testSubstituteWithReferenceToNonExistentParameter() {
        final List<ParameterToken> referenceList = new ArrayList<>();
        referenceList.add(new StandardParameterReference("foo", 0, 5, "#{foo}"));

        final ParameterLookup paramContext = Mockito.mock(ParameterLookup.class);
        Mockito.when(paramContext.getParameter(Mockito.anyString())).thenReturn(Optional.empty());
        final StandardParameterTokenList references = new StandardParameterTokenList("#{foo}", referenceList);

        assertEquals("#{foo}", references.substitute(paramContext));
    }

    @Test
    public void testSubstituteWithEscapes() {
        final List<ParameterToken> referenceList = new ArrayList<>();
        referenceList.add(new StartCharacterEscape(0));
        referenceList.add(new EscapedParameterReference(2, 8, "##{foo}"));

        final ParameterLookup paramContext = Mockito.mock(ParameterLookup.class);
        Mockito.when(paramContext.getParameter("foo")).thenReturn(Optional.of(new Parameter(new ParameterDescriptor.Builder().name("foo").build(), "bar")));

        StandardParameterTokenList references = new StandardParameterTokenList("####{foo}", referenceList);
        assertEquals("##{foo}", references.substitute(paramContext));

        referenceList.add(new StandardParameterReference("foo", 12, 17, "#{foo}"));
        references = new StandardParameterTokenList("####{foo}***#{foo}", referenceList);
        assertEquals("##{foo}***bar", references.substitute(paramContext));
    }

    @Test
    public void testEscape() {
        final List<ParameterToken> referenceList = new ArrayList<>();

        assertEquals("Hello", new StandardParameterTokenList("Hello", referenceList).escape());

        referenceList.add(new StandardParameterReference("abc", 0, 5, "#{abc}"));
        assertEquals("##{abc}", new StandardParameterTokenList("#{abc}", referenceList).escape());

        referenceList.clear();
        referenceList.add(new EscapedParameterReference(0, 6, "##{abc}"));
        assertEquals("####{abc}", new StandardParameterTokenList("##{abc}", referenceList).escape());

        referenceList.clear();
        referenceList.add(new StartCharacterEscape(0));
        referenceList.add(new StandardParameterReference("abc", 2, 7, "#{abc}"));
        assertEquals("######{abc}", new StandardParameterTokenList("###{abc}", referenceList).escape());

        referenceList.clear();
        referenceList.add(new StartCharacterEscape(0));
        referenceList.add(new EscapedParameterReference(2, 8, "##{abc}"));
        assertEquals("########{abc}", new StandardParameterTokenList("####{abc}", referenceList).escape());
    }
}

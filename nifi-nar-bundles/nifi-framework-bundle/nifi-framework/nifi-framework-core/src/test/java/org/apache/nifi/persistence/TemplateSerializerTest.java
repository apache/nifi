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
package org.apache.nifi.persistence;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.apache.nifi.nar.NarClassLoader;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.diff.HistogramDiff;
import org.eclipse.jgit.diff.RawText;
import org.eclipse.jgit.diff.RawTextComparator;
import org.junit.Before;
import org.junit.Test;

public class TemplateSerializerTest {
    @Before
    public void before() throws Exception {
        Field initField = NarClassLoaders.class.getDeclaredField("initialized");
        setFinalField(initField, new AtomicBoolean(true));
        Field clField = NarClassLoaders.class.getDeclaredField("frameworkClassLoader");
        NarClassLoader cl = new NarClassLoader(new File(""), Thread.currentThread().getContextClassLoader());
        setFinalField(clField, new AtomicReference<NarClassLoader>(cl));
    }

    @Test
    public void validateDiffWithChangingComponentIdAndAdditionalElements() throws Exception {

        // Create initial template (TemplateDTO)
        FlowSnippetDTO snippet = new FlowSnippetDTO();
        Set<ProcessorDTO> procs = new HashSet<>();
        for (int i = 4; i > 0; i--) {
            ProcessorDTO procDTO = new ProcessorDTO();
            procDTO.setType("Processor" + i + ".class");
            procDTO.setId("Proc-" + i);
            procs.add(procDTO);
        }
        snippet.setProcessors(procs);
        TemplateDTO origTemplate = new TemplateDTO();
        origTemplate.setDescription("MyTemplate");
        origTemplate.setId("MyTemplate");
        origTemplate.setSnippet(snippet);
        byte[] serTemplate = TemplateSerializer.serialize(origTemplate);

        // Deserialize Template into TemplateDTP
        ByteArrayInputStream in = new ByteArrayInputStream(serTemplate);
        JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        JAXBElement<TemplateDTO> templateElement = unmarshaller.unmarshal(new StreamSource(in), TemplateDTO.class);
        TemplateDTO deserTemplate = templateElement.getValue();

        // Modify deserialized template
        FlowSnippetDTO deserSnippet = deserTemplate.getSnippet();
        Set<ProcessorDTO> deserProcs = deserSnippet.getProcessors();
        int c = 0;
        for (ProcessorDTO processorDTO : deserProcs) {
            processorDTO.setId(processorDTO.getId() + "-" + c);
            if (c % 2 == 0) {
                processorDTO.setName("Hello-" + c);
            }
            c++;
        }

        // add new Processor
        ProcessorDTO procDTO = new ProcessorDTO();
        procDTO.setType("ProcessorNew" + ".class");
        procDTO.setId("ProcNew-" + 10);
        deserProcs.add(procDTO);

        // Serialize modified template
        byte[] serTemplate2 = TemplateSerializer.serialize(deserTemplate);

        RawText rt1 = new RawText(serTemplate);
        RawText rt2 = new RawText(serTemplate2);
        EditList diffList = new EditList();
        diffList.addAll(new HistogramDiff().diff(RawTextComparator.DEFAULT, rt1, rt2));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DiffFormatter diff = new DiffFormatter(out);) {
            diff.format(diffList, rt1, rt2);

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new ByteArrayInputStream(out.toByteArray()), StandardCharsets.UTF_8));

            List<String> changes = reader.lines()
                .peek(System.out::println)
                .filter(line -> line.startsWith("+") || line.startsWith("-"))
                .collect(Collectors.toList());

            assertEquals("-      <id>Proc-2</id>", changes.get(0));
            assertEquals("+      <id>Proc-2-0</id>", changes.get(1));
            assertEquals("+      <name>Hello-0</name>", changes.get(2));

            assertEquals("-      <id>Proc-4</id>", changes.get(3));
            assertEquals("+      <id>Proc-4-1</id>", changes.get(4));

            assertEquals("-      <id>Proc-1</id>", changes.get(5));
            assertEquals("+      <id>Proc-1-2</id>", changes.get(6));
            assertEquals("+      <name>Hello-2</name>", changes.get(7));

            assertEquals("-      <id>Proc-3</id>", changes.get(8));
            assertEquals("+      <id>Proc-3-3</id>", changes.get(9));

            assertEquals("+    <processors>", changes.get(10));
            assertEquals("+      <id>ProcNew-10</id>", changes.get(12));
            assertEquals("+      <type>ProcessorNew.class</type>", changes.get(13));
            assertEquals("+    </processors>", changes.get(14));
        }
    }

    public static void setFinalField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }
}

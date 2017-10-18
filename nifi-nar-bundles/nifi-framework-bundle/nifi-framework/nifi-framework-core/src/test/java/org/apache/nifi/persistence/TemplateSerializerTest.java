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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamReader;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.diff.HistogramDiff;
import org.eclipse.jgit.diff.RawText;
import org.eclipse.jgit.diff.RawTextComparator;
import org.junit.Test;

public class TemplateSerializerTest {

    @Test
    public void validateDiffWithChangingComponentIdAndAdditionalElements() throws Exception {

        // Create initial template (TemplateDTO)
        FlowSnippetDTO snippet = new FlowSnippetDTO();
        Set<ProcessorDTO> procs = new HashSet<>();
        for (int i = 4; i > 0; i--) {
            ProcessorDTO procDTO = new ProcessorDTO();
            procDTO.setType("Processor" + i + ".class");
            procDTO.setId(ComponentIdGenerator.generateId().toString());
            procs.add(procDTO);
        }
        snippet.setProcessors(procs);
        TemplateDTO origTemplate = new TemplateDTO();
        origTemplate.setDescription("MyTemplate");
        origTemplate.setId("MyTemplate");
        origTemplate.setSnippet(snippet);
        byte[] serTemplate = TemplateSerializer.serialize(origTemplate);

        // Deserialize Template into TemplateDTO
        ByteArrayInputStream in = new ByteArrayInputStream(serTemplate);
        JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        XMLStreamReader xsr = XmlUtils.createSafeReader(in);
        JAXBElement<TemplateDTO> templateElement = unmarshaller.unmarshal(xsr, TemplateDTO.class);
        TemplateDTO deserTemplate = templateElement.getValue();

        // Modify deserialized template
        FlowSnippetDTO deserSnippet = deserTemplate.getSnippet();
        Set<ProcessorDTO> deserProcs = deserSnippet.getProcessors();
        int c = 0;
        for (ProcessorDTO processorDTO : deserProcs) {
            if (c % 2 == 0) {
                processorDTO.setName("Hello-" + c);
            }
            c++;
        }

        // add new Processor
        ProcessorDTO procDTO = new ProcessorDTO();
        procDTO.setType("ProcessorNew" + ".class");
        procDTO.setId(ComponentIdGenerator.generateId().toString());
        deserProcs.add(procDTO);

        // Serialize modified template
        byte[] serTemplate2 = TemplateSerializer.serialize(deserTemplate);

        RawText rt1 = new RawText(serTemplate);
        RawText rt2 = new RawText(serTemplate2);
        EditList diffList = new EditList();
        diffList.addAll(new HistogramDiff().diff(RawTextComparator.DEFAULT, rt1, rt2));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DiffFormatter diff = new DiffFormatter(out)) {
            diff.format(diffList, rt1, rt2);

            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(out.toByteArray()), StandardCharsets.UTF_8));

            List<String> changes = reader.lines().peek(System.out::println)
                    .filter(line -> line.startsWith("+") || line.startsWith("-")).collect(Collectors.toList());

            assertEquals("+      <name>Hello-0</name>", changes.get(0));
            assertEquals("+      <name>Hello-2</name>", changes.get(1));
            assertEquals("+    <processors>", changes.get(2));
            assertEquals("+      <type>ProcessorNew.class</type>", changes.get(4));
            assertEquals("+    </processors>", changes.get(5));
        }
    }

}
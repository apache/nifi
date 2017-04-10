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
package org.apache.nifi.processors.ccda;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openhealthtools.mdht.uml.cda.consol.ConsolFactory;
import org.openhealthtools.mdht.uml.cda.consol.ContinuityOfCareDocument;
import org.openhealthtools.mdht.uml.cda.consol.ProblemConcernAct;
import org.openhealthtools.mdht.uml.cda.consol.ProblemObservation;
import org.openhealthtools.mdht.uml.cda.consol.ProblemSection;
import org.openhealthtools.mdht.uml.cda.consol.ProblemStatus;
import org.openhealthtools.mdht.uml.cda.consol.VitalSignObservation;
import org.openhealthtools.mdht.uml.cda.consol.VitalSignsOrganizer;
import org.openhealthtools.mdht.uml.cda.consol.VitalSignsSection;
import org.openhealthtools.mdht.uml.cda.util.CDAUtil;


public class TestExtractCCDAAttributes {

    private TestRunner runner;

    @BeforeClass
    public static void setup() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "INFO");
    }

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(ExtractCCDAAttributes.class);
    }

    @Test
    public void testProcessor() throws Exception {
        Map<String, String> expectedAttributes = new HashMap<String, String>();
        expectedAttributes.put("code.code", "34133-9");
        expectedAttributes.put("code.codeSystem", "2.16.840.1.113883.6.1");
        expectedAttributes.put("code.codeSystemName", "LOINC");
        expectedAttributes.put("code.displayName", "Summarization of Episode Note");
        expectedAttributes.put("problemSection.code.code", "11450-4");
        expectedAttributes.put("problemSection.code.codeSystem", "2.16.840.1.113883.6.1");
        expectedAttributes.put("problemSection.code.codeSystemName", "LOINC");
        expectedAttributes.put("problemSection.code.displayName", "Problem List");
        expectedAttributes.put("problemSection.act.code.code", "CONC");
        expectedAttributes.put("problemSection.act.code.codeSystem", "2.16.840.1.113883.5.6");
        expectedAttributes.put("problemSection.act.code.codeSystemName", "HL7ActClass");
        expectedAttributes.put("problemSection.act.code.displayName", "Concern");
        expectedAttributes.put("problemSection.act.observation.problemStatus.code.code", "33999-4");
        expectedAttributes.put("problemSection.act.observation.problemStatus.code.codeSystem", "2.16.840.1.113883.6.1");
        expectedAttributes.put("problemSection.act.observation.problemStatus.code.codeSystemName", "LOINC");
        expectedAttributes.put("problemSection.act.observation.problemStatus.code.displayName", "Status");
        expectedAttributes.put("problemSection.act.observation.problemStatus.statusCode.code", "completed");
        expectedAttributes.put("problemSection.act.observation.statusCode.code", "completed");
        expectedAttributes.put("vitalSignsSection.code.code", "8716-3");
        expectedAttributes.put("vitalSignsSection.code.codeSystem", "2.16.840.1.113883.6.1");
        expectedAttributes.put("vitalSignsSection.code.codeSystemName", "LOINC");
        expectedAttributes.put("vitalSignsSection.code.displayName", "Vital Signs");
        expectedAttributes.put("vitalSignsSection.organizer.code.code", "46680005");
        expectedAttributes.put("vitalSignsSection.organizer.code.codeSystem", "2.16.840.1.113883.6.96");
        expectedAttributes.put("vitalSignsSection.organizer.code.codeSystemName", "SNOMEDCT");
        expectedAttributes.put("vitalSignsSection.organizer.code.displayName", "Vital signs");
        expectedAttributes.put("vitalSignsSection.organizer.statusCode.code", "completed");
        expectedAttributes.put("vitalSignsSection.organizer.observations.code.codeSystem", "2.16.840.1.113883.6.1");
        expectedAttributes.put("vitalSignsSection.organizer.observations.code.codeSystemName", "LOINC");
        expectedAttributes.put("vitalSignsSection.organizer.observations.statusCode.code", "completed");

        ContinuityOfCareDocument doc = ConsolFactory.eINSTANCE.createContinuityOfCareDocument().init();

        ProblemConcernAct problemAct = ConsolFactory.eINSTANCE.createProblemConcernAct().init();
        ProblemObservation problemObservation = ConsolFactory.eINSTANCE.createProblemObservation().init();
        ProblemStatus problemStatus = ConsolFactory.eINSTANCE.createProblemStatus().init();

        ProblemSection problemSection = ConsolFactory.eINSTANCE.createProblemSection().init();
        doc.addSection(problemSection);
        problemSection.addAct(problemAct);
        problemAct.addObservation(problemObservation);
        problemObservation.addObservation(problemStatus);

        VitalSignsOrganizer vitalSignsOrganizer = ConsolFactory.eINSTANCE.createVitalSignsOrganizer().init();
        VitalSignObservation vitalSignObservation = ConsolFactory.eINSTANCE.createVitalSignObservation().init();

        VitalSignsSection vitalSignsSection = ConsolFactory.eINSTANCE.createVitalSignsSection().init();
        doc.addSection(vitalSignsSection);
        vitalSignsSection.addOrganizer(vitalSignsOrganizer);
        vitalSignsOrganizer.addObservation(vitalSignObservation);

        StringWriter writer = new StringWriter();
        CDAUtil.save(doc, writer);

        runTests(writer.toString(), expectedAttributes, true, true);
    }

    private void runTests(final String content, Map<String, String> expectedAttributes, final boolean skipValidation, final boolean prettyPrinting) throws IOException{
        runner.setProperty(ExtractCCDAAttributes.SKIP_VALIDATION, String.valueOf(skipValidation));

        runner.enqueue(content);

        runner.run();
        runner.assertAllFlowFilesTransferred(ExtractCCDAAttributes.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractCCDAAttributes.REL_SUCCESS).get(0);
        for (final Map.Entry<String, String> entry : expectedAttributes.entrySet()) {
            flowFile.assertAttributeEquals(entry.getKey(), entry.getValue());
        }

    }
}

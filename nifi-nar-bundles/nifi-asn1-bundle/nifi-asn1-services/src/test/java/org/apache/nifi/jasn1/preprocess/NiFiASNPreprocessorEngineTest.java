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
package org.apache.nifi.jasn1.preprocess;

import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NiFiASNPreprocessorEngineTest {
    private NiFiASNPreprocessorEngine testSubject;
    private NiFiASNPreprocessorEngine helper;

    private NiFiASNPreprocessor mockPreprocessor1;
    private NiFiASNPreprocessor mockPreprocessor2;
    private List<NiFiASNPreprocessor> preprocessors;

    private ComponentLog log;

    @BeforeEach
    void setUp() {
        mockPreprocessor1 = mock(NiFiASNPreprocessor.class);
        mockPreprocessor2 = mock(NiFiASNPreprocessor.class);

        preprocessors = Arrays.asList(
                mockPreprocessor1,
                mockPreprocessor2
        );

        log = mock(ComponentLog.class);

        helper = mock(NiFiASNPreprocessorEngine.class);
        testSubject = new NiFiASNPreprocessorEngine() {
            @Override
            List<String> readAsnLines(ComponentLog componentLog, String inputFile, Path inputFilePath) {
                return helper.readAsnLines(componentLog, inputFile, inputFilePath);
            }

            @Override
            void writePreprocessedAsn(ComponentLog componentLog, String preprocessedAsn, Path preprocessedAsnPath) {
                helper.writePreprocessedAsn(componentLog, preprocessedAsn, preprocessedAsnPath);
            }

            @Override
            List<NiFiASNPreprocessor> getPreprocessors() {
                return preprocessors;
            }
        };
    }

    @Test
    void testPreprocess() {
        // GIVEN
        String asnFilesString = "/path/to/asn_file_1,/path/to/asn_file_2";
        String outputDirectory = "/path/to/directory_for_transformed_asn_files";

        List<String> originalLines1 = Arrays.asList("original_lines_1_1","original_lines_1_2");
        List<String> preprocessedLines1_1 = Arrays.asList("preprocessed_lines_1_1_1","preprocessed_lines_1_1_2");
        List<String> preprocessedLines1_2 = Arrays.asList("final_lines_1_1", "final_lines_1_2");

        List<String> originalLines2 = Arrays.asList("original_lines_2_1","original_lines_2_2");
        List<String> preprocessedLines2_1 = Arrays.asList("preprocessed_lines_2_1_1", "preprocessed_lines_2_1_2");
        List<String> preprocessedLines2_2 = Arrays.asList("final_lines_2_1", "final_lines_2_2");

        when(helper.readAsnLines(eq(log), eq("/path/to/asn_file_1"), eq(Paths.get("/path/to/asn_file_1"))))
                .thenReturn(originalLines1);
        when(mockPreprocessor1.preprocessAsn(originalLines1)).thenReturn(preprocessedLines1_1);
        when(mockPreprocessor2.preprocessAsn(preprocessedLines1_1)).thenReturn(preprocessedLines1_2);

        when(helper.readAsnLines(eq(log), eq("/path/to/asn_file_2"), eq(Paths.get("/path/to/asn_file_2"))))
                .thenReturn(originalLines2);
        when(mockPreprocessor1.preprocessAsn(originalLines2)).thenReturn(preprocessedLines2_1);
        when(mockPreprocessor2.preprocessAsn(preprocessedLines2_1)).thenReturn(preprocessedLines2_2);

        String expected = "/path/to/directory_for_transformed_asn_files/asn_file_1,/path/to/directory_for_transformed_asn_files/asn_file_2";

        // WHEN
        String actual = testSubject.preprocess(log, asnFilesString, outputDirectory);

        // THEN
        assertEquals(expected, actual);

        verify(helper).readAsnLines(eq(log), eq("/path/to/asn_file_1"), eq(Paths.get("/path/to/asn_file_1")));
        verify(helper).readAsnLines(eq(log), eq("/path/to/asn_file_2"), eq(Paths.get("/path/to/asn_file_2")));

        verify(mockPreprocessor1).preprocessAsn(originalLines1);
        verify(mockPreprocessor2).preprocessAsn(preprocessedLines1_1);

        verify(mockPreprocessor1).preprocessAsn(originalLines2);
        verify(mockPreprocessor2).preprocessAsn(preprocessedLines2_1);

        verify(helper).writePreprocessedAsn(
                eq(log),
                eq("final_lines_1_1" + System.lineSeparator() + "final_lines_1_2"),
                eq(Paths.get("/path/to/directory_for_transformed_asn_files/asn_file_1"))
        );

        verify(helper).writePreprocessedAsn(
                eq(log),
                eq("final_lines_2_1" + System.lineSeparator() + "final_lines_2_2"),
                eq(Paths.get("/path/to/directory_for_transformed_asn_files/asn_file_2"))
        );
    }
}

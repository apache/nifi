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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsnPreprocessorEngineTest {
    private AsnPreprocessorEngine testSubject;
    private AsnPreprocessorEngine helper;

    private AsnPreprocessor mockPreprocessor1;
    private AsnPreprocessor mockPreprocessor2;
    private List<AsnPreprocessor> preprocessors;

    private ComponentLog log;

    @TempDir
    private File additionalPreprocessingOutputDirectory;

    @BeforeEach
    void setUp() throws Exception {
        mockPreprocessor1 = mock(AsnPreprocessor.class);
        mockPreprocessor2 = mock(AsnPreprocessor.class);

        preprocessors = Arrays.asList(
                mockPreprocessor1,
                mockPreprocessor2
        );

        log = mock(ComponentLog.class);

        helper = mock(AsnPreprocessorEngine.class);
        testSubject = new AsnPreprocessorEngine() {
            @Override
            List<String> readAsnLines(ComponentLog componentLog, String inputFile, Path inputFilePath) {
                return helper.readAsnLines(componentLog, inputFile, inputFilePath);
            }

            @Override
            void writePreprocessedAsn(ComponentLog componentLog, String preprocessedAsn, Path preprocessedAsnPath) {
                helper.writePreprocessedAsn(componentLog, preprocessedAsn, preprocessedAsnPath);
            }

            @Override
            List<AsnPreprocessor> getPreprocessors() {
                return preprocessors;
            }
        };
    }

    @Test
    void testPreprocess() {
        // GIVEN
        Path asnFile1Path = Paths.get("path", "to", "asn_file_1");
        Path asnFile2Path = Paths.get("path", "to", "asn_file_2");

        String asnFilesString = new StringJoiner(",")
                .add(asnFile1Path.toString())
                .add(asnFile2Path.toString())
                .toString();

        String outputDirectory = Paths.get("path", "to", "directory_for_transformed_asn_files").toString();

        List<String> originalLines1 = Arrays.asList("original_lines_1_1", "original_lines_1_2");
        List<String> preprocessedLines1_1 = Arrays.asList("preprocessed_lines_1_1_1", "preprocessed_lines_1_1_2");
        List<String> preprocessedLines1_2 = Arrays.asList("final_lines_1_1", "final_lines_1_2");

        List<String> originalLines2 = Arrays.asList("original_lines_2_1", "original_lines_2_2");
        List<String> preprocessedLines2_1 = Arrays.asList("preprocessed_lines_2_1_1", "preprocessed_lines_2_1_2");
        List<String> preprocessedLines2_2 = Arrays.asList("final_lines_2_1", "final_lines_2_2");

        when(helper.readAsnLines(eq(log), eq(asnFile1Path.toString()), eq(asnFile1Path)))
                .thenReturn(originalLines1);
        when(mockPreprocessor1.preprocessAsn(originalLines1)).thenReturn(preprocessedLines1_1);
        when(mockPreprocessor2.preprocessAsn(preprocessedLines1_1)).thenReturn(preprocessedLines1_2);

        when(helper.readAsnLines(eq(log), eq(asnFile2Path.toString()), eq(asnFile2Path)))
                .thenReturn(originalLines2);
        when(mockPreprocessor1.preprocessAsn(originalLines2)).thenReturn(preprocessedLines2_1);
        when(mockPreprocessor2.preprocessAsn(preprocessedLines2_1)).thenReturn(preprocessedLines2_2);

        String expected = new StringJoiner(",")
                .add(Paths.get("path", "to", "directory_for_transformed_asn_files", "asn_file_1").toString())
                .add(Paths.get("path", "to", "directory_for_transformed_asn_files", "asn_file_2").toString())
                .toString();

        // WHEN
        String actual = testSubject.preprocess(log, asnFilesString, outputDirectory);

        // THEN
        assertEquals(expected, actual);

        verify(helper).readAsnLines(eq(log), eq(asnFile1Path.toString()), eq(asnFile1Path));
        verify(helper).readAsnLines(eq(log), eq(asnFile2Path.toString()), eq(asnFile2Path));

        verify(mockPreprocessor1).preprocessAsn(originalLines1);
        verify(mockPreprocessor2).preprocessAsn(preprocessedLines1_1);

        verify(mockPreprocessor1).preprocessAsn(originalLines2);
        verify(mockPreprocessor2).preprocessAsn(preprocessedLines2_1);

        verify(helper).writePreprocessedAsn(
                eq(log),
                eq("final_lines_1_1" + System.lineSeparator() + "final_lines_1_2"),
                eq(Paths.get("path", "to", "directory_for_transformed_asn_files", "asn_file_1"))
        );

        verify(helper).writePreprocessedAsn(
                eq(log),
                eq("final_lines_2_1" + System.lineSeparator() + "final_lines_2_2"),
                eq(Paths.get("path", "to", "directory_for_transformed_asn_files", "asn_file_2"))
        );
    }

    @Test
    void testComplexPreprocessing() throws Exception {
        testSubject = new AsnPreprocessorEngine();

        String input = "test_complex_for_preprocessing.asn";


        String preprocessedFile = testSubject.preprocess(
                log,
                new File(getClass().getClassLoader().getResource(input).toURI()).getAbsolutePath(),
                additionalPreprocessingOutputDirectory.getAbsolutePath()
        );

        String expected = new String(Files.readAllBytes(Paths.get(getClass().getClassLoader().getResource("preprocessed_" + input).toURI())), StandardCharsets.UTF_8)
                .replace("\n", System.lineSeparator());

        String actual = new String(Files.readAllBytes(Paths.get(preprocessedFile)), StandardCharsets.UTF_8);

        assertEquals(expected, actual);
    }
}

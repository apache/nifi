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
package org.apache.nifi.processors.groovyx;

import groovy.json.JsonOutput;
import groovy.json.JsonSlurper;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.embedded.database.EmbeddedDatabaseConnectionService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.codehaus.groovy.runtime.ResourceGroovyMethods;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExecuteGroovyScriptTest {
    protected TestRunner runner;
    protected static EmbeddedDatabaseConnectionService dbcp = null;  //to make single initialization
    protected MockRecordParser recordParser = null;
    protected RecordSetWriterFactory recordWriter = null;
    protected ExecuteGroovyScript proc;
    public final String TEST_RESOURCE_LOCATION = "target/test/resources/groovy/";
    private final String TEST_CSV_DATA = """
            gender,title,first,last
            female,miss,marlene,shaw
            male,mr,todd,graham""";

    /**
     * Copies all scripts to the target directory because when they are compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @BeforeAll
    public static void setupBeforeClass(@TempDir final Path tempDir) throws Exception {
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test/resources"));

        dbcp = new EmbeddedDatabaseConnectionService(tempDir);
        try (Connection con = dbcp.getConnection()) {
            executeStatement(con, "drop table if exists mytable");
            executeStatement(con, "create table mytable (id integer not null, name varchar(100), scale float, created timestamp, data blob)");
            executeStatement(con, "insert into mytable (id, name, scale, created, data) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234', null)");
            executeStatement(con, "insert into mytable (id, name, scale, created, data) VALUES (1, 'Carrie Jones', 5.1, '2000-01-01 03:23:34.234', null)");
        }
    }

    @AfterAll
    public static void close() throws IOException {
        dbcp.close();
    }

    private static void executeStatement(final Connection connection, final String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        //init processor
        proc = new ExecuteGroovyScript();
        MockProcessContext context = new MockProcessContext(proc);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(proc, context);
        proc.initialize(initContext);

        assertNotNull(proc.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(proc);
        runner.addControllerService("dbcp", dbcp, new HashMap<>());
        runner.enableControllerService(dbcp);

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("code", RecordFieldType.INT.getDataType()));

        recordParser = new MockRecordParser();
        recordFields.forEach((r) -> recordParser.addSchemaField(r));
        runner.addControllerService("myreader", recordParser, new HashMap<>());
        runner.enableControllerService(recordParser);

        recordWriter = new MockRecordWriter();
        runner.addControllerService("mywriter", recordWriter, new HashMap<>());
        runner.enableControllerService(recordWriter);
    }

    /**
     * Tests a script that reads content of the flowfile content and stores the value in an attribute of the outgoing flowfile.
     *
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, "def flowFile = session.get(); if(!flowFile)return; flowFile.testAttr = flowFile.read().getText('UTF-8'); REL_SUCCESS << flowFile;");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        result.getFirst().assertAttributeEquals("testAttr", "test content");
    }

    @Test
    public void testAdditionalClasspath(@TempDir File tempDir) throws Exception {
        Set<URL> expectedClasspathURLs = new HashSet<>();
        StringBuilder additionalClasspath = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            Path p = new File(tempDir, getClass().getName() + UUID.randomUUID() + ".tmp").toPath();
            Files.createFile(p);
            expectedClasspathURLs.add(p.toUri().toURL());
            additionalClasspath.append(p);
            additionalClasspath.append(i == 0 ? ',' : ';'); // create additional classpath string separated by ; and ,
        }

        runner.setProperty(ExecuteGroovyScript.ADD_CLASSPATH, additionalClasspath.toString());
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, ";");
        runner.assertValid();

        URL[] classpathURLs = proc.shell.getClassLoader().getURLs();
        assertEquals(expectedClasspathURLs, new HashSet<>(Arrays.asList(classpathURLs)));
    }

    @Test
    void testNonExistingAdditionalClasspath() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, ";");
        runner.setProperty(ExecuteGroovyScript.ADD_CLASSPATH, "someOtherPath");
        runner.assertNotValid();

        final MockComponentLog logger = runner.getLogger();
        assertEquals(1, logger.getWarnMessages().size());
        assertTrue(logger.getWarnMessages().getFirst().getMsg().contains("not found"));
    }

    @Test
    public void test_onTrigger_groovy() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_onTrigger.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();

        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        result.getFirst().assertAttributeEquals("from-content", "test content");
    }

    @Test
    public void test_onTriggerX_groovy() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_onTriggerX.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();

        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        result.getFirst().assertAttributeEquals("from-content", "test content");
    }

    @Test
    public void test_onTrigger_changeContent_groovy() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_onTrigger_changeContent.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();

        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertAttributeEquals("selected.columns", "first,last");
        resultFile.assertContentEquals("Marlene Shaw\nTodd Graham\n");
    }

    @Test
    public void test_onTrigger_changeContentX_groovy() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_onTrigger_changeContentX.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();

        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertAttributeEquals("selected.columns", "first,last");
        resultFile.assertContentEquals("Marlene Shaw\nTodd Graham\n");
    }

    @Test
    public void test_no_input_groovy() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_no_input.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertAttributeEquals("filename", "test.txt");
        resultFile.assertContentEquals("Test");
    }


    @Test
    public void test_good_script() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, " def ff = session.get(); if(!ff)return; REL_SUCCESS << ff ");
        runner.assertValid();
    }

    @Test
    public void test_bad_script() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, " { { ");
        runner.assertNotValid();
    }

    //---------------------------------------------------------
    @Test
    public void test_ctl_01_access() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_ctl_01_access.groovy");
        runner.setProperty("CTL.mydbcp", "dbcp"); //pass dbcp as a service to script
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertContentEquals("OK", StandardCharsets.UTF_8);
    }

    @Test
    public void test_sql_01_select() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_sql_01_select.groovy");
        runner.setProperty("SQL.mydb", "dbcp");
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertAttributeEquals("filename", "test.txt");
        resultFile.assertContentEquals("Joe Smith\nCarrie Jones\n", StandardCharsets.UTF_8);
    }

    @Test
    public void test_sql_02_blob_write() throws Exception {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_sql_02_blob_write.groovy");
        runner.setProperty("SQL.mydb", "dbcp");
        runner.setProperty("ID", "0");
        runner.assertValid();

        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8), map("ID", "0"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertContentEquals(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void test_sql_03_blob_read() throws Exception {
        //read blob from database written at previous step and write to flow file
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_sql_03_blob_read.groovy");
        runner.setProperty("SQL.mydb", "dbcp");
        runner.setProperty("ID", "0");
        runner.setValidateExpressionUsage(false);
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertContentEquals(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void test_sql_04_insert_and_json() throws Exception {
        //read blob from database written at previous step and write to flow file
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_sql_04_insert_and_json.groovy");
        runner.setProperty("SQL.mydb", "dbcp");
        runner.setValidateExpressionUsage(false);
        runner.assertValid();

        runner.enqueue(new FileInputStream(TEST_RESOURCE_LOCATION + "test_sql_04_insert_and_json.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 3);  //number of inserted rows
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        List<String> lines = ResourceGroovyMethods.readLines(new File(TEST_RESOURCE_LOCATION + "test_sql_04_insert_and_json.json"), "UTF-8");
        //pass through to&from json before compare
        resultFile.assertContentEquals(JsonOutput.toJson(new JsonSlurper().parseText(lines.get(1))), StandardCharsets.UTF_8);
    }

    @Test
    public void test_record_reader_writer_access() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_record_reader_writer.groovy");
        runner.setProperty("RecordReader.myreader", "myreader"); //pass myreader as a service to script
        runner.setProperty("RecordWriter.mywriter", "mywriter"); //pass mywriter as a service to script
        runner.assertValid();

        recordParser.addRecord(1, "A", "XYZ");
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertContentEquals("\"1\",\"A\",\"XYZ\"\n", StandardCharsets.UTF_8);
    }

    @Test
    public void test_filter_01() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, "def ff = session.get{it.FILTER=='3'}; if(!ff)return; REL_SUCCESS << ff;");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");

        runner.assertValid();

        runner.enqueue("01".getBytes(StandardCharsets.UTF_8), map("FILTER", "1"));
        runner.enqueue("31".getBytes(StandardCharsets.UTF_8), map("FILTER", "3"));
        runner.enqueue("03".getBytes(StandardCharsets.UTF_8), map("FILTER", "2"));
        runner.enqueue("32".getBytes(StandardCharsets.UTF_8), map("FILTER", "3"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 2);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());

        result.get(0).assertContentEquals("31", StandardCharsets.UTF_8);
        result.get(1).assertContentEquals("32", StandardCharsets.UTF_8);
    }

    @Test
    public void test_read_01() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, "def ff = session.get(); if(!ff)return; assert ff.read().getText('UTF-8')=='1234'; REL_SUCCESS << ff; ");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
    }

    @Test
    public void test_read_02() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, "def ff = session.get(); if(!ff)return; ff.read{s-> assert s.getText('UTF-8')=='1234' }; REL_SUCCESS << ff; ");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
    }

    @Test
    public void test_read_03() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, "def ff = session.get(); if(!ff)return; ff.read('UTF-8'){r-> assert r.getText()=='1234' }; REL_SUCCESS << ff; ");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
    }

    @Test
    public void test_withInputStream() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, """
                import java.util.stream.*
                def ff = session.get(); if(!ff)return;
                ff.withInputStream{inputStream -> String r = new BufferedReader(new InputStreamReader(inputStream)).lines()\
                .collect(Collectors.joining("\\n")); assert r=='1234' }; REL_SUCCESS << ff;\s""");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
    }

    @Test
    public void test_withOutputStream() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, """
                import java.util.stream.*
                def ff = session.get(); if(!ff)return;
                ff.withOutputStream{outputStream -> outputStream.write('5678'.bytes)}; REL_SUCCESS << ff;\s""");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals("5678");
    }

    @Test
    public void test_withReader() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, """
                import java.util.stream.*
                def ff = session.get(); if(!ff)return;
                ff.withReader('UTF-8'){reader -> String r = new BufferedReader(reader).lines()\
                .collect(Collectors.joining("\\n")); assert r=='1234' }; REL_SUCCESS << ff;\s""");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
    }

    @Test
    public void test_withWriter() throws Exception {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, """
                import java.util.stream.*
                def ff = session.get(); if(!ff)return;
                ff.withWriter('UTF-16LE'){writer -> writer.write('5678')}; REL_SUCCESS << ff;\s""");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals("5678".getBytes(StandardCharsets.UTF_16LE));
    }

    @Test
    public void test_withInputStreamReturnsClosureValue() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, """
                import java.util.stream.*
                def ff = session.get(); if(!ff)return;
                def outputBody = ff.withInputStream{inputStream -> '5678'}
                ff.withOutputStream{outputStream -> outputStream.write(outputBody.bytes)}; REL_SUCCESS << ff;\s""");

        runner.assertValid();

        runner.enqueue("1234".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals("5678");
    }

    @Test
    public void test_onStart_onStop() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_onStart_onStop.groovy");
        runner.assertValid();
        runner.enqueue("");
        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.getFirst();
        resultFile.assertAttributeExists("a");
        resultFile.assertAttributeEquals("a", "A");
        System.setOut(originalOut);
        assertEquals(getExpectedContent("onStop invoked successfully\n"), outContent.toString());

        // Inspect the output visually for onStop, no way to pass back values
    }

    @Test
    public void test_onUnscheduled() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_onUnscheduled.groovy");
        runner.assertValid();
        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        runner.run();
        System.setOut(originalOut);
        assertEquals(getExpectedContent("onUnscheduled invoked successfully\n"), outContent.toString());
    }

    @Test
    public void test_attribute_passed_to_SQL() {
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "test_attributes_passed_to_SQL.groovy");
        runner.setProperty("database.name", "testDB");
        runner.setProperty("SQL.mydb", "dbcp");
        runner.assertValid();

        runner.run();
    }

    @Test
    public void test_sensitive_dynamic_property() {
        new PropertyDescriptor.Builder()
                .name("password")
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .sensitive(true)
                .build();
        runner.setProperty("password", "MyP@ssW0rd!");
        runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY,
                "assert context.getProperties().find {k,v -> k.name == 'password'}.key.sensitive");
        runner.assertValid();
        runner.run();
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("groovyx-script-file", ExecuteGroovyScript.SCRIPT_FILE.getName()),
                Map.entry("groovyx-script-body", ExecuteGroovyScript.SCRIPT_BODY.getName()),
                Map.entry("groovyx-failure-strategy", ExecuteGroovyScript.FAIL_STRATEGY.getName()),
                Map.entry("groovyx-additional-classpath", ExecuteGroovyScript.ADD_CLASSPATH.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private Map<String, String> map(String key, String value) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(key, value);
        return attrs;
    }

    private static String getExpectedContent(String string) {
        final boolean windows = System.getProperty("os.name").startsWith("Windows");
        String expectedContent = string;

        if (windows) {
            expectedContent = expectedContent.replaceAll("\n", "\r\n");
        }

        return expectedContent;
    }
}

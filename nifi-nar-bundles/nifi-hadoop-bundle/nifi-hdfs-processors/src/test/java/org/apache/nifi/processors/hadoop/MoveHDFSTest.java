package org.apache.nifi.processors.hadoop;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MoveHDFSTest {

	private static final String OUTPUT_DIRECTORY = "src/test/resources/testdataoutput";
	private static final String INPUT_DIRECTORY = "src/test/resources/testdata";
	private static final String DOT_FILE_PATH = "src/test/resources/testdata/.testfordotfiles";
	private NiFiProperties mockNiFiProperties;
	private KerberosProperties kerberosProperties;

	@Before
	public void setup() {
		mockNiFiProperties = mock(NiFiProperties.class);
		when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
		kerberosProperties = new KerberosProperties(null);
	}

	@After
	public void teardown() {
		File outputDirectory = new File(OUTPUT_DIRECTORY);
		if (outputDirectory.exists()) {
			if (outputDirectory.isDirectory()) {
				moveFilesFromOutputDirectoryToInput();
			}
			outputDirectory.delete();
		}
		removeDotFile();
	}

	private void removeDotFile() {
		File dotFile = new File(DOT_FILE_PATH);
		if (dotFile.exists()) {
			dotFile.delete();
		}
	}

	private void moveFilesFromOutputDirectoryToInput() {
		File folder = new File(OUTPUT_DIRECTORY);
		for (File file : folder.listFiles()) {
			if (file.isFile()) {
				String path = file.getAbsolutePath();
				if(!path.endsWith(".crc")) {
					String newPath = path.replaceAll("testdataoutput", "testdata");
					File newFile = new File(newPath);
					if (!newFile.exists()) {
						file.renameTo(newFile);
					}
				} else {
					file.delete();
				}
			}
		}
	}

	@Test
	public void testOutputDirectoryValidator() {
		MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
		TestRunner runner = TestRunners.newTestRunner(proc);
		Collection<ValidationResult> results;
		ProcessContext pc;

		results = new HashSet<>();
		runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, "/source");
		runner.enqueue(new byte[0]);
		pc = runner.getProcessContext();
		if (pc instanceof MockProcessContext) {
			results = ((MockProcessContext) pc).validate();
		}
		Assert.assertEquals(1, results.size());
		for (ValidationResult vr : results) {
			assertTrue(vr.toString().contains("Output Directory is required"));
		}
	}

	@Test
	public void testBothInputAndOutputDirectoriesAreValid() {
		MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
		TestRunner runner = TestRunners.newTestRunner(proc);
		Collection<ValidationResult> results;
		ProcessContext pc;

		results = new HashSet<>();
		runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
		runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
		runner.enqueue(new byte[0]);
		pc = runner.getProcessContext();
		if (pc instanceof MockProcessContext) {
			results = ((MockProcessContext) pc).validate();
		}
		Assert.assertEquals(0, results.size());
	}

	@Test
	public void testOnScheduledShouldRunCleanly() {
		MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
		TestRunner runner = TestRunners.newTestRunner(proc);
		runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
		runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
		runner.enqueue(new byte[0]);
		runner.setValidateExpressionUsage(false);
		runner.run();
		List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
		runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
		Assert.assertEquals(7, flowFiles.size());
	}
	
	@Test
	public void testDotFileFilter() throws IOException {
		createDotFile();
		MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
		TestRunner runner = TestRunners.newTestRunner(proc);
		runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
		runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
		runner.setProperty(MoveHDFS.IGNORE_DOTTED_FILES, "false");
		runner.enqueue(new byte[0]);
		runner.setValidateExpressionUsage(false);
		runner.run();
		List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
		runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
		Assert.assertEquals(8, flowFiles.size());
	}
	
	@Test
	public void testFileFilterRegex() {
		MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
		TestRunner runner = TestRunners.newTestRunner(proc);
		runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
		runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
		runner.setProperty(MoveHDFS.FILE_FILTER_REGEX, ".*\\.gz");
		runner.enqueue(new byte[0]);
		runner.setValidateExpressionUsage(false);
		runner.run();
		List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
		runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
		Assert.assertEquals(1, flowFiles.size());
	}
	
	@Test
	public void testSingleFileAsInput() {
		MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
		TestRunner runner = TestRunners.newTestRunner(proc);
		runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY + "/randombytes-1");
		runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
		runner.enqueue(new byte[0]);
		runner.setValidateExpressionUsage(false);
		runner.run();
		List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
		runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
		Assert.assertEquals(1, flowFiles.size());
	}

	private void createDotFile() throws IOException {
		File dotFile = new File(DOT_FILE_PATH);
		dotFile.createNewFile();
	}

	private static class TestableMoveHDFS extends MoveHDFS {

		private KerberosProperties testKerberosProperties;

		public TestableMoveHDFS(KerberosProperties testKerberosProperties) {
			this.testKerberosProperties = testKerberosProperties;
		}

		@Override
		protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
			return testKerberosProperties;
		}

	}

}

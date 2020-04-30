package org.apache.nifi.processors.h2o.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
//import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExecuteMojoScoringRecord {

	private TestRunner runner;
	private ExecuteMojoScoringRecord processor;
	private MockRecordParser readerService;
	private CSVRecordSetWriter writerService;
	
	// The pretty printed json comparisons don't work on windows
	@BeforeClass
	public static void setUpSuit() {
		Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
	}
	
	@Before
	public void setup() throws Exception {
		processor = new ExecuteMojoScoringRecord();
		runner = TestRunners.newTestRunner(processor);
		readerService = new MockRecordParser();
		
		try {
			runner.addControllerService("reader", readerService);
		} catch(InitializationException e) {
			throw new IOException(e);
		}
		runner.enableControllerService(readerService);
		runner.setProperty(ExecuteMojoScoringRecord.RECORD_READER, "reader");
		
		writerService = new CSVRecordSetWriter();
		try {
			runner.addControllerService("writer", writerService);
		} catch(InitializationException e) {
			throw new IOException(e);
		}
		
		runner.setProperty(ExecuteMojoScoringRecord.RECORD_WRITER, "writer");
		// Each test must set the Schema Access Strategy and Schema
		// and enable the writer Controller Service
	}
	
	@Test
	public void testRelationshipsCreated() throws IOException {
		generateHydraulicTestData(1, null);
		
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecord/mojo2-runtime.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		
		// Include the filepath to pipeline.mojo that predicts the label for hydraulic cooling condition
		final String pipelineMojoPath = "src/test/resources/TestExecuteMojoScoringRecord/hydraulic/coolingCondition/pipeline.mojo";
		runner.setProperty(ExecuteMojoScoringRecord.PIPELINE_MOJO_FILEPATH, pipelineMojoPath);
		runner.enqueue(new byte[0]);
		
		Set<Relationship> relationships = processor.getRelationships();
		assertTrue(relationships.contains(ExecuteMojoScoringRecord.REL_FAILURE));
		assertTrue(relationships.contains(ExecuteMojoScoringRecord.REL_SUCCESS));
		assertTrue(relationships.contains(ExecuteMojoScoringRecord.REL_ORIGINAL));
		assertEquals(3, relationships.size());
	}
	
	@Test
	public void testInvalidModulePath() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		// Include the invalid filepath to mojo2-runtime.jar
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecrd/mojo2-runtmie.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		runner.assertNotValid();
	}
	
	@Test
	public void testModulePathIsEmpty() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, StringUtils.EMPTY);
		runner.assertNotValid();
	}
	
	@Test
	public void testInvalidPipelineMojoPath() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecord/mojo2-runtime.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		
		// Include the invalid filepath to pipeline.mojo
		final String pipelineMojoPath = "";
		runner.setProperty(ExecuteMojoScoringRecord.PIPELINE_MOJO_FILEPATH, pipelineMojoPath);
		runner.enqueue(new byte[0]);
		runner.assertNotValid();
	}
	
	@Test
	public void testPipelineMojoPathIsEmpty() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecord/mojo2-runtime.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		
		runner.setProperty(ExecuteMojoScoringRecord.PIPELINE_MOJO_FILEPATH, StringUtils.EMPTY);
		runner.assertNotValid();
	}
	
	@Test
	public void testNoFlowFileContent() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecord/mojo2-runtime.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		
		// Include the filepath to pipeline.mojo that predicts the label for hydraulic cooling condition
		final String pipelineMojoPath = "src/test/resources/TestExecuteMojoScoringRecord/hydraulic/coolingCondition/pipeline.mojo";
		runner.setProperty(ExecuteMojoScoringRecord.PIPELINE_MOJO_FILEPATH, pipelineMojoPath);
		
		runner.run();
		runner.assertQueueEmpty();
		runner.assertTransferCount(ExecuteMojoScoringRecord.REL_FAILURE, 0);
		runner.assertTransferCount(ExecuteMojoScoringRecord.REL_SUCCESS, 0);
	}
	
	@Test
	public void testNoRecords() throws IOException {
		generateHydraulicTestData(0, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecord/mojo2-runtime.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		
		// Include the filepath to pipeline.mojo that predicts the label for hydraulic cooling condition
		final String pipelineMojoPath = "src/test/resources/TestExecuteMojoScoringRecord/hydraulic/coolingCondition/pipeline.mojo";
		runner.setProperty(ExecuteMojoScoringRecord.PIPELINE_MOJO_FILEPATH, pipelineMojoPath);
		
		runner.enqueue("");
		runner.run();
		runner.assertQueueEmpty();
		runner.assertTransferCount(ExecuteMojoScoringRecord.REL_FAILURE, 0);
		runner.assertTransferCount(ExecuteMojoScoringRecord.REL_SUCCESS, 1);
		runner.assertTransferCount(ExecuteMojoScoringRecord.REL_ORIGINAL, 1);
	}
	
	@Test
	public void testInvalidFlowFileContent() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecord/mojo2-runtime.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		
		// Include the filepath to pipeline.mojo that predicts the label for hydraulic cooling condition
		final String pipelineMojoPath = "src/test/resources/TestExecuteMojoScoringRecord/hydraulic/coolingCondition/pipeline.mojo";
		runner.setProperty(ExecuteMojoScoringRecord.PIPELINE_MOJO_FILEPATH, pipelineMojoPath);
		
		readerService.failAfter(0);
		runner.enqueue("invalid csv");
		runner.run();

		runner.assertAllFlowFilesTransferred(ExecuteMojoScoringRecord.REL_FAILURE);
	}
	
	@Test
	public void testPredictionWithNoModule() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		// Include the filepath to pipeline.mojo that predicts the label for hydraulic cooling condition
		final String pipelineMojoPath = "src/test/resources/TestExecuteMojoScoringRecord/hydraulic/coolingCondition/pipeline.mojo";
		runner.setProperty(ExecuteMojoScoringRecord.PIPELINE_MOJO_FILEPATH, pipelineMojoPath);
		runner.assertNotValid();
	}
	
	@Test
	public void testPredictionWithNoPipelineMojo() throws IOException {
		generateHydraulicTestData(1, null);
		// "Inherit Record Schema" is set by default for CSVRecordSetWriter SCHEMA_ACCESS_STRATEGY property, so it commented out
		// runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
		runner.enableControllerService(writerService);
		
		final String mojo2RuntimeJarPath = "src/test/resources/TestExecuteMojoScoringRecord/mojo2-runtime.jar";
		runner.setProperty(ExecuteMojoScoringRecord.MODULE, mojo2RuntimeJarPath);
		runner.assertNotValid();
	}
	
	private void generateHydraulicTestData(int numRecords, final BiFunction<Integer, MockRecordParser, Void> recordGenerator) {
		
		// based on hydraulicTestData.avsc file
		if(recordGenerator == null) {
			final RecordSchema hydraulicTestSchema = new SimpleRecordSchema(
				Arrays.asList(
						new RecordField("psa_bar", RecordFieldType.FLOAT.getDataType()),
						new RecordField("psb_bar", RecordFieldType.FLOAT.getDataType()),
						new RecordField("psc_bar", RecordFieldType.FLOAT.getDataType()),
						new RecordField("psd_bar", RecordFieldType.FLOAT.getDataType()),
						new RecordField("pse_bar", RecordFieldType.FLOAT.getDataType()),
						new RecordField("psf_bar", RecordFieldType.FLOAT.getDataType()),
						new RecordField("fsa_vol_flow", RecordFieldType.FLOAT.getDataType()),
						new RecordField("fsb_vol_flow", RecordFieldType.FLOAT.getDataType()),
						new RecordField("tsa_temp", RecordFieldType.FLOAT.getDataType()),
						new RecordField("tsb_temp", RecordFieldType.FLOAT.getDataType()),
						new RecordField("tsc_temp", RecordFieldType.FLOAT.getDataType()),
						new RecordField("tsd_temp", RecordFieldType.FLOAT.getDataType()),
						new RecordField("pump_eff", RecordFieldType.FLOAT.getDataType()),
						new RecordField("vs_vib", RecordFieldType.FLOAT.getDataType()),
						new RecordField("cool_pwr_pct", RecordFieldType.FLOAT.getDataType()),
						new RecordField("eff_fact_pct", RecordFieldType.FLOAT.getDataType())
				)
			);
			
			readerService.addSchemaField("hydraulicTestData", RecordFieldType.RECORD);
			
			Random r = new Random();
			
			for(int i = 0; i < numRecords; i++) {
				
				Record hydraulicTestRecord = new MapRecord(hydraulicTestSchema,
					new HashMap<String, Object>() {{
						put("psa_bar", randFloat(155.391540f, 180.922714f));
						put("psb_bar", randFloat(104.406303f, 131.589096f));
						put("psc_bar", randFloat(0.840252f, 2.021872f));
						put("psd_bar", randFloat(0.0f, 10.207067f));
						put("pse_bar", randFloat(8.365800f, 9.978510f));
						put("psf_bar", randFloat(8.321526f, 9.856591f));
						put("fsa_vol_flow", randFloat(2.018571f, 6.722706f));
						put("fsb_vol_flow", randFloat(8.857513f, 10.366250f));
						put("tsa_temp", randFloat(35.313781f, 57.899284f));
						put("tsb_temp", randFloat(40.859401f, 61.958465f));
						put("tsc_temp", randFloat(38.245735f, 59.423168f));
						put("tsd_temp", randFloat(30.390800f, 53.060417f));
						put("pump_eff", randFloat(2361.747314f, 2740.641113f));
						put("vs_vib", randFloat(0.524366f, 0.839066f));
						put("cool_pwr_pct", randFloat(1.072083f, 2.840100f));
						put("eff_fact_pct", randFloat(18.276617f, 60.737049f));
					}}
				);
				
				readerService.addRecord(hydraulicTestRecord);
			}
			
		}
		else {
			recordGenerator.apply(numRecords,  readerService);
		}
	}
	
	private float randFloat(float min, float max) {
		Random rand = new Random();
		return rand.nextFloat() * (max-min) + min;
	}
}

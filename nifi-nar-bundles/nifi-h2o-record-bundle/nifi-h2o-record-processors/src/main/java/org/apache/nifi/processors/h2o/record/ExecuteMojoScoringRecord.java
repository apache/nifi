package org.apache.nifi.processors.h2o.record;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StopWatch;

// Require using MODULE property to dynamically load in MOJO2 Runtime JAR (ex: mojo2-runtime.jar)
import ai.h2o.mojos.runtime.MojoPipeline;
import ai.h2o.mojos.runtime.frame.MojoFrame;
import ai.h2o.mojos.runtime.frame.MojoFrameBuilder;
import ai.h2o.mojos.runtime.frame.MojoColumn;
import ai.h2o.mojos.runtime.frame.MojoRowBuilder;
import ai.h2o.mojos.runtime.lic.LicenseException;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"record", "execute", "mojo", "scoring", "predictions", "driverless ai", "h2o", "machine learning"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
	@WritesAttribute(attribute = "record.count", description = "The number of records in an outgoing FlowFile"),
	@WritesAttribute(attribute = "mime.type", description = "The MIME Type that the configured Record Writer indicates is appropriate"),
})
@CapabilityDescription("Executes H2O's Driverless AI MOJO Scoring Pipeline in Java Runtime to do batch "
		+ "scoring or real time scoring for one or more predicted label(s) on the tabular test data in "
		+ "the incoming flow file content. If tabular data is one row, then MOJO does real time scoring. "
		+ "If tabular data is multiple rows, then MOJO does batch scoring. For this processor, you will "
		+ "need a Driverless AI license key, so it can execute the Driverless AI Mojo.")
@RequiresInstanceClassLoading
public class ExecuteMojoScoringRecord extends AbstractProcessor {

	static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
			.name("h2o-record-record-reader")
			.displayName("Record Reader")
			.description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
			.identifiesControllerService(RecordReaderFactory.class)
			.required(true)
			.build();
	
	static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
			.name("h2o-record-record-writer")
			.displayName("Record Writer")
			.description("Specifies the Controller Service to use for writing out the records")
			.identifiesControllerService(RecordSetWriterFactory.class)
			.required(true)
			.build();
	
	public static final PropertyDescriptor MODULE = new PropertyDescriptor.Builder()
			.name("h2o-record-custom-modules")
			.displayName("MOJO2 Runtime JAR Directory")
			.description("Path to the file or directory which contains the JAR (ex: mojo2-runtime.jar) containing modules to " 
					+ "execute the MOJO to do scoring (that are not included on NiFi's classpath)")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.NONE)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.dynamicallyModifiesClasspath(true)
			.build();
	
	public static final PropertyDescriptor PIPELINE_MOJO_FILEPATH = new PropertyDescriptor.Builder()
			.name("h2o-record-pipeline-mojo-filepath")
			.displayName("Pipeline MOJO Filepath")
			.description("Path to the pipeline.mojo. This file will be used with the custom MOJO2 runtime JAR modules to instantiate MOJOPipeline object.")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.NONE)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("The FlowFile with prediction content will be routed to this relationship")
			.build();
	
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("If a FlowFile fails processing for any reason (for example, the FlowFile records cannot be parsed), it will be routed to this relationship")
			.build();
	
	public static final Relationship REL_ORIGINAL = new Relationship.Builder()
			.name("original")
			.description("The original FlowFile that was scored. If the FlowFile fails processing, nothing will be sent to this relationship")
			.build();
			
	private final static List<PropertyDescriptor> properties;
	private final static Set<Relationship> relationships;
			
	static {
		ArrayList<PropertyDescriptor> _properties = new ArrayList<>();
		_properties.add(RECORD_READER);
		_properties.add(RECORD_WRITER);
		_properties.add(MODULE);
		_properties.add(PIPELINE_MOJO_FILEPATH);
		properties = Collections.unmodifiableList(_properties);
		
		final Set<Relationship> _relationships = new HashSet<>();
		_relationships.add(REL_SUCCESS);
		_relationships.add(REL_FAILURE);
		_relationships.add(REL_ORIGINAL);
		relationships = Collections.unmodifiableSet(_relationships);
	}
	
	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}
	
	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}
	
	@Override
	protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
		final String pipelineMojoPath = validationContext.getProperty(PIPELINE_MOJO_FILEPATH).isSet() ? validationContext.getProperty(PIPELINE_MOJO_FILEPATH).getValue() : null;
		
		if(pipelineMojoPath == null) {
			final String message = "A Pipeline MOJO filepath is required to instantiate MOJOPipeline object";
			results.add(new ValidationResult.Builder().valid(false)
				   .explanation(message)
				   .build());
		}
		
		return results;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final FlowFile original = session.get();
		
		if(original == null) {
			return;
		}
		
		final ComponentLog logger = getLogger();
		final StopWatch stopWatch = new StopWatch(true);
		
		final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
		final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
		
		final RecordSchema schema;
		FlowFile scored = null; // flowfile contents contains scored (predicted) data
		
		try (final InputStream in = session.read(original);
			 final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())
			) {
			schema = writerFactory.getSchema(original.getAttributes(), reader.getSchema());
			
			final Map<String, String> attributes = new HashMap<>();
			final WriteResult writeResult;
			scored = session.create(original);
			
			// We want to score the first record before creating the Record Writer. We do this because the Record will 
			// likely end up with a different structure and therefore a different Schema after being scored. As a result,
			// we want to score the Record and then provide the scored schema to the Record Writer so that if the Record
			// Writer chooses to inherit the Record Schema from the Record itself, it will inherit the scored schema, not
			// the schema determined by the Record Reader
			final Record firstRecord = reader.nextRecord();
			
			if(firstRecord == null) {
				try (final OutputStream out = session.write(scored);
					 final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, scored)
					) {
					writer.beginRecordSet();
					writeResult = writer.finishRecordSet();
					
					attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
					attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
					attributes.putAll(writeResult.getAttributes());
				}
				
				scored = session.putAllAttributes(scored, attributes);
				logger.info("{} had no Records to score", new Object[]{original});
			}
			else {
				
				final String pipelineMojoPath = context.getProperty(PIPELINE_MOJO_FILEPATH).getValue();
				logger.info("Got mojo filepath: " + pipelineMojoPath);
				
				// Load Mojo Pipeline (includes feature engineering + ML model)
				MojoPipeline model = MojoPipeline.loadFrom(pipelineMojoPath);
				final String mojoPipelineUUID = "pipeline.mojo uuid " + model.getUuid();
				logger.info("loaded mojo and has UUID: " + mojoPipelineUUID);
				
				final Record scoredFirstRecord = predict(firstRecord, model, getLogger());
				
				if(scoredFirstRecord == null) {
					throw new ProcessException("Error scoring the first record");
				}
				
				final RecordSchema writeSchema = writerFactory.getSchema(original.getAttributes(), scoredFirstRecord.getSchema());
				
				try (final OutputStream out = session.write(scored);
					final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, scored)
				    ) {
					writer.beginRecordSet();
					
					writer.write(scoredFirstRecord);
					
					Record record;
					record = reader.nextRecord();
					
					while(record != null) {
						logger.info("processing next record in the stream");
						final Record scoredRecord = predict(record, model, getLogger());
						logger.info("writing scored record");
						writer.write(scoredRecord);
						record = reader.nextRecord();
					}
					
					writeResult = writer.finishRecordSet();

					try {
						writer.close();
					} catch (final IOException ioe) {
						getLogger().warn("Failed to close Writer for {}", new Object[]{scored});
					}
					
					attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
					attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
					attributes.putAll(writeResult.getAttributes());
				}
				
				
				scored = session.putAllAttributes(scored, attributes);
				session.getProvenanceReporter().modifyContent(scored, "Modified With " + mojoPipelineUUID, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
				logger.debug("Scored {}", new Object[] {original});
				
			}
		} catch (final Exception ex) {
			logger.error("Unable to score {} due to", new Object[]{original});
			session.transfer(original, REL_FAILURE);
			if (scored != null) {
				session.remove(scored);
			}
			return;
		}
		if (scored != null) {
			logger.info("Transferring flow file on rel_success with bytes = " + scored.getSize());
			session.transfer(scored, REL_SUCCESS);
		}
		session.transfer(original, REL_ORIGINAL);
	}
	
	@SuppressWarnings("unchecked")
	private Record predict(final Record record, MojoPipeline model, final ComponentLog logger) {
		Map<String, Object> recordMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
		
		// Get an instance of a MojoFrameBuilder that will be used to make an input frame
		MojoFrameBuilder frameBuilder = model.getInputFrameBuilder();
		// Get an instance of a MojoRowBuilder that will be used to construct a row for this builder
		MojoRowBuilder rowBuilder = frameBuilder.getMojoRowBuilder();
		
		logger.info("Processing input recordMap into input MojoFrame iframe");
		for(Map.Entry<String, Object> recordEntry: recordMap.entrySet()) {
			logger.info("Key = " + recordEntry.getKey() + ", Value = " + recordEntry.getValue());
			// Set a value to the position associated with the column name in the row
			rowBuilder.setValue(recordEntry.getKey(), String.valueOf(recordEntry.getValue()));
		}
		// Append a row from the current state of the rowBuilder
		frameBuilder.addRow(rowBuilder);
		
		// Construct a MojoFrame iframe which will be transformed by MOJO pipeline
		MojoFrame iframe = frameBuilder.toMojoFrame();
		
		// Executes the pipeline of transformers as stated in this model's Mojo file
		// In other words, use Mojo to predict the labels from iframe real world data
		MojoFrame oframe = model.transform(iframe);
		
		// Create a new map of key value pair(s) based on the predictions from oframe
		Map<String, Object> predictedRecordMap = new HashMap<>();
		
		logger.info("Processing oframe into predictedRecordMap:");
		// loop on the number of rows of each column in the MojoFrame
		for(int row_i = 0; row_i < oframe.getNrows(); row_i++)
		{
			// loop on the number of columns in each row in the MojoFrame
			for(int col_j = 0; col_j < oframe.getNcols(); col_j++)
			{
				// Get the name of a column at a particular index getColumnName(int index)
				String key = oframe.getColumnName(col_j);
				
				// Get the data stored in the column at a particular index getColumnData(int index)
				Object columnData = oframe.getColumnData(col_j);
				
				logger.info("col_j = " + col_j + ", Key = " + key);
				
				// Check the Data Type in column at certain index using MojoColumn.Type enum
				// Cast Object to appropriate Data Type, then store in predictedRecordMap
				if(oframe.getColumnType(col_j) == MojoColumn.Type.Bool) {
					boolean[] columnDataArray = (boolean[])columnData;
					predictedRecordMap.put(key, columnDataArray[row_i]);
					logger.info("Bool Value = " + Boolean.toString(columnDataArray[row_i]));
				}
				else if(oframe.getColumnType(col_j) == MojoColumn.Type.Int32) {
					int[] columnDataArray = (int[])columnData;
					predictedRecordMap.put(key, columnDataArray[row_i]);
					logger.info("Int32 Value = " + String.valueOf(columnDataArray[row_i]));
				}
				else if(oframe.getColumnType(col_j) == MojoColumn.Type.Int64) {
					long[] columnDataArray = (long[])columnData;
					predictedRecordMap.put(key, columnDataArray[row_i]);
					logger.info("Int64 Value = " + String.valueOf(columnDataArray[row_i]));
				}
				else if(oframe.getColumnType(col_j) == MojoColumn.Type.Float32)
				{
					float[] columnDataArray = (float[])columnData;
					predictedRecordMap.put(key, columnDataArray[row_i]);
					logger.info("Float32 Value = " + String.valueOf(columnDataArray[row_i]));
				}
				else if(oframe.getColumnType(col_j) == MojoColumn.Type.Float64)
				{
					double[] columnDataArray = (double[])columnData;
					predictedRecordMap.put(key, columnDataArray[row_i]);
					logger.info("Float64 Value = " + String.valueOf(columnDataArray[row_i]));
				}
				else if(oframe.getColumnType(col_j) == MojoColumn.Type.Str)
				{
					String[] columnDataArray = (String[])columnData;
					predictedRecordMap.put(key, columnDataArray[row_i]);
					logger.info("Str Value = " + columnDataArray[row_i]);
				}
				else if(oframe.getColumnType(col_j) == MojoColumn.Type.Time64)
				{
					float[] columnDataArray = (float[])columnData;
					predictedRecordMap.put(key, columnDataArray[row_i]);
					logger.info("Time64 Value = " + String.valueOf(columnDataArray[row_i]));
				}	
				// TODO: Look into adding support for MojoColumn.Type Time64, MojoDateTime[] object
				else {
					logger.error("Mojo Column Type is not supported.");
				}
			}
		}

		final Record predictedRecord = DataTypeUtils.toRecord(predictedRecordMap, "r");
		return predictedRecord;
	}
}

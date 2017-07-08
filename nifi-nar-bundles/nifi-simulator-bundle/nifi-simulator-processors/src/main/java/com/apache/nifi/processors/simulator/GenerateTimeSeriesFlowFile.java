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
package com.apache.nifi.processors.simulator;

import be.cetic.tsimulus.config.Configuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.LocalDateTime;
import scala.Some;
import scala.Tuple3;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;
import java.util.ArrayList;

@Tags({"Simulator, Timeseries, IOT, Testing"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Generates realistic time series data using the TSimulus time series generator, and places the values into the flowfile in a CSV format.")
public class GenerateTimeSeriesFlowFile extends AbstractProcessor {

    private Configuration simConfig = null;
    private boolean isTest = false;

    public static final PropertyDescriptor SIMULATOR_CONFIG = new PropertyDescriptor
            .Builder().name("SIMULATOR_CONFIG")
            .displayName("Simulator Configuration File")
            .description("The JSON configuration file to use to configure TSimulus")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRINT_HEADER = new PropertyDescriptor
            .Builder().name("PRINT_HEADER")
            .displayName("Print Header")
            .description("Directs the processor whether to print a header line or not.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("When the flowfile is successfully generated")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();

        descriptors.add(SIMULATOR_CONFIG);
        descriptors.add(PRINT_HEADER);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (SIMULATOR_CONFIG.equals(descriptor))
            simConfig = null;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        loadConfiguration(context.getProperty(SIMULATOR_CONFIG).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();

        // Create the flowfile, as it probably does not exist
        if (flowFile == null)
            flowFile = session.create();

        // Get the data
        String data = generateData(context.getProperty(PRINT_HEADER).asBoolean());

        // Write the results back out to flow file
        try{
            flowFile = session.write(flowFile, out -> out.write(data.getBytes()));
            session.getProvenanceReporter().create(flowFile);
            session.transfer(flowFile, SUCCESS);
        } catch (ProcessException ex) {
            logger.error("Unable to write generated data out to flowfile. Error: ", ex);
        }
    }

    // Loads the configuration from the file
    private void loadConfiguration(String fileName){
        if (simConfig == null){
            // Load the simulator configuration
            if (fileName.contains("/configs/unitTestConfig.json"))
                isTest = true;
            try{
                simConfig = SimController.getConfiguration(fileName);
            }catch (Exception ex){
                getLogger().error("Error loading configuration: " + ex.getMessage());
                throw ex;
            }

        }
    }

    // Actually do the data generation via TSimulus
    private String generateData(boolean printHeader){
        LocalDateTime queryTime = LocalDateTime.now();
        if(isTest)
            queryTime = LocalDateTime.parse("2016-01-01T00:00:00.000");

        // Get the time Values for the current time
        scala.collection.Iterable<Tuple3<String, LocalDateTime, Object>> data = SimController.getTimeValue(simConfig.timeSeries(), queryTime);

        // Convert the Scala Iterable to a Java one
        Iterable<Tuple3<String, LocalDateTime, Object>> generatedValues = JavaConverters.asJavaIterableConverter(data).asJava();

        // Build the flow file string
        StringBuilder dataValueString = new StringBuilder();

        if (printHeader)
            dataValueString.append("name, ts, value").append(System.lineSeparator());

        generatedValues.forEach(tv -> {
            String dataValue = ((Some)tv._3()).get().toString();
            dataValueString.append(tv._1()).append(",").append(tv._2().toString()).append(",").append(dataValue);
            dataValueString.append(System.lineSeparator());
        });

        return dataValueString.toString().trim();
    }
}

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
package org.apache.nifi.processors.rwfile;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.rwfile.WriteFileService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
/**
 * @author every
 */
@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"append", "local", "copy", "archive", "files", "filesystem"})
@CapabilityDescription("Writes the contents of a FlowFile to the local file system")
@ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to disk.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                        explanation = "Provides operator the ability to write to any file that NiFi has access to.")
        }
)
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class WriteProcessor extends AbstractProcessor {

    public static final PropertyDescriptor FILEPATH = (new PropertyDescriptor.Builder())
            .name("File to Fetch")
            .description("The fully-qualified filename of the file to fetch from the file system")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${absolute.path}/${filename}")
            .required(true)
            .build();
    public static final PropertyDescriptor SOURCE_CHARSET = (new PropertyDescriptor.Builder())
            .name("source character-set")
            .displayName("Source Character Set")
            .description("The Character Encoding that is used to encode/decode the file")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(true)
            .build();
    public static final PropertyDescriptor DESTINATION_CHARSET = (new PropertyDescriptor.Builder())
            .name("destination file character-set")
            .displayName("Destination Character Set")
            .description("The Character Encoding that is used to encode/decode the file")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(true)
            .build();
    public static final PropertyDescriptor WRITEFILE_SERVICE = new PropertyDescriptor
            .Builder().name("WRITEFILE_SERVICE")
            .displayName("write file service")
            .description("WRITE file Property")
            .required(true)
            .identifiesControllerService(WriteFileService.class)
            .build();
    public static final PropertyDescriptor COUNT = new PropertyDescriptor
            .Builder().name("file count")
            .displayName("write file count")
            .description("write file count Property")
            .required(true)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_ORIGINAL = (new Relationship.Builder())
            .name("original")
            .description("The original file")
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();
    public static final Relationship REL_WRITE_SUCCESS = new Relationship.Builder()
            .name("write file over")
            .description("Files that write all rows have been successfully written to the output directory are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        // relationships
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_ORIGINAL);
        procRels.add(REL_SUCCESS);
        procRels.add(REL_WRITE_SUCCESS);
        procRels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(FILEPATH);
        supDescriptors.add(SOURCE_CHARSET);
        supDescriptors.add(DESTINATION_CHARSET);
        supDescriptors.add(WRITEFILE_SERVICE);
        supDescriptors.add(COUNT);
        properties = Collections.unmodifiableList(supDescriptors);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        long start = System.currentTimeMillis();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String srcCharSet = context.getProperty(SOURCE_CHARSET).evaluateAttributeExpressions(flowFile).getValue();
        String desCharSet = context.getProperty(DESTINATION_CHARSET).evaluateAttributeExpressions(flowFile).getValue();
        String filepath = context.getProperty(FILEPATH).evaluateAttributeExpressions(flowFile).getValue();
        WriteFileService writeFileService = context.getProperty(WRITEFILE_SERVICE).asControllerService(WriteFileService.class);
        int fragmentCount=context.getProperty(COUNT).evaluateAttributeExpressions(flowFile).asInteger();

        Map writeData;
        try(InputStream is =session.read(flowFile)) {
            InputStreamReader isr=new InputStreamReader(is, srcCharSet);
            BufferedReader br = new BufferedReader(isr);

            String line = null;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                if (!desCharSet.equals(srcCharSet)) {
                    line = new String(line.getBytes(srcCharSet), desCharSet);
                }
                sb.append(line ).append("\n");
            }

            writeData = writeFileService.writeFile(filepath,sb.toString(), desCharSet);

            getLogger().debug("write data to file"+(System.currentTimeMillis()-start)+" " +start );
            if ((boolean)writeData.get("over")) {
                session.transfer(session.putAllAttributes(session.create(flowFile), flowFile.getAttributes()), REL_SUCCESS);
            }else{
                session.transfer(session.putAllAttributes(session.create(flowFile), flowFile.getAttributes()), REL_FAILURE);
            }

            synchronized (writeFileService){
                Long writeCount = (Long)writeData.get("writeCount");
                if(writeCount.intValue()==fragmentCount){
                    writeFileService.closeFile(filepath);
                    session.transfer(session.putAllAttributes(session.create(flowFile), flowFile.getAttributes()), REL_WRITE_SUCCESS);
                }
            }
        }catch (IOException ioe){
            getLogger().error("Could not fetch file from file system for {} ", new Object[]{ ioe.toString()}, ioe);
            session.transfer(session.putAllAttributes(session.create(flowFile), flowFile.getAttributes()), REL_FAILURE);
        }catch (Throwable e){
            getLogger().error("",e);
            session.transfer(session.putAllAttributes(session.create(flowFile), flowFile.getAttributes()), REL_FAILURE);
        }
        session.transfer(flowFile, REL_ORIGINAL);

    }

}

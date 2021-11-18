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
package org.apache.nifi.processors.standard;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Tags({"hex", "dump", "convert", "flowfile"})
@WritesAttributes({@WritesAttribute(attribute="first16hex", description="A customizable Hexdump of incoming " +
" FlowFiles. The attributes length, offset and name can be altered.")})
@CapabilityDescription("Converts incoming FlowFiles into hexadecimal format. The processor can output the HexDump " +
    "data in new FlowFiles or/and as an renamable Attribute.")
public class HexDump extends AbstractProcessor {

    public static final PropertyDescriptor OUT_MODE = new PropertyDescriptor
            .Builder().name("OUT_MODE")
            .displayName("Output Hexdump Mode")
            .description("Selectable Mode, describing if the converted Hexdump data should yield FlowFile data, Attribute or"+
            " Both.")
            .required(true)
            .defaultValue("BOTH")
            .allowableValues("FLOWFILE","ATTRIBUTE","BOTH")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FLOWFILE_OFFSET = new PropertyDescriptor
            .Builder().name("FLOWFILE_OFFSET")
            .displayName("FlowFile output offset")
            .description("Non negative integer offset of Hexdump in FlowFile output. If offset is out of bounds no "+
            " data will be transferred. This is only used if mode is FLOWFILE or BOTH")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor FLOWFILE_LENGTH = new PropertyDescriptor
            .Builder().name("FLOWFILE_LENGTH")
            .displayName("FlowFile output length")
            .description("Non negative integer describing maximum size of Hexdump in FlowFile output. 0 = unlimited. "+
            "This is only used if Mode is FLOWFILE or BOTH.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor
            .Builder().name("ATTRIBUTE_NAME")
            .displayName("Attribute hexdump name")
            .description("String Hexdump output attribute name. This is only used if mode is ATTRIBUTE or BOTH.")
            .required(true)
            .defaultValue("first16hex")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_OFFSET = new PropertyDescriptor
            .Builder().name("ATTRIBUTE_OFFSET")
            .displayName("Attribute Hexdump Offset")
            .description("Non negative integer offset of attribute Hexdump. If offset is out of bounds no data will be "+
            " transferred. This is only used if mode is ATTRIBUTE or BOTH.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_LENGTH = new PropertyDescriptor
            .Builder().name("ATTRIBUTE_LENGTH")
            .displayName("Attribute Hexdump Length")
            .description("Non negative integer maximum size of attribute Hexdump. This is only used if mode is ATTRIBUTE or"+
            " BOTH.")
            .required(true)
            .defaultValue("16")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted FlowFile to Hexdump")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to convert FlowFile to Hexdump")
            .build();

    private static final char hex[] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
        };
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(OUT_MODE);
        descriptors.add(FLOWFILE_LENGTH);
        descriptors.add(FLOWFILE_OFFSET);
        descriptors.add(ATTRIBUTE_NAME);
        descriptors.add(ATTRIBUTE_OFFSET);
        descriptors.add(ATTRIBUTE_LENGTH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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

    private String outMode;
    private int flowFileLength;
    private int flowFileOffset;
    private String attributeName;
    private int attributeOffset;
    private int attributeLength;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        outMode = context.getProperty(OUT_MODE).getValue();
        flowFileLength = context.getProperty(FLOWFILE_LENGTH).asInteger();
        flowFileOffset = context.getProperty(FLOWFILE_OFFSET).asInteger();
        attributeName = context.getProperty(ATTRIBUTE_NAME).getValue();
        attributeLength = context.getProperty(ATTRIBUTE_LENGTH).asInteger();
        attributeOffset = context.getProperty(ATTRIBUTE_OFFSET).asInteger();
    }

     /**
     * Read incoming FlowFile and convert to Hexdump array
     *
     * @return
     *  Hexdump converted byte array
     */
    private byte[] readFlowFileToHex(ProcessSession session, FlowFile flowFile) throws IOException{
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final InputStream in = new BufferedInputStream( session.read(flowFile));
            while (true) {
                final int nextByte = in.read();
                if (nextByte == -1) {
                    break;
                }
                buffer.write(hex[((nextByte >> 4) & 0xF)]);
                buffer.write(hex[((nextByte >> 0) & 0xF)]);
            }
            in.close();
            return buffer.toByteArray();
    }

    private boolean offsetOutOfBounds(int[] index){
        return index[0] == -1;
    }

     /**
     * Calculate limits for data from offsets and lengths
     *
     * @return
     *  Array of size 2 describing max and min of Hexdump array interval
     */
    private int[] getArrayLimits(int offset, int length, int arrayLength, int intervalLimit){
        int start = offset;
        int end = length + offset;

        if (length == 0){
            end = arrayLength;
        }

        if (start > arrayLength){
            // Offset out of bounds
            start = -1;
        }

        if (end > arrayLength){
            end = arrayLength;
        }

        if (intervalLimit > 0 && end > intervalLimit ){
            end = intervalLimit;
        }

        return new int[] {start, end};
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {
            boolean transferFlowFile = false;
            byte[] hexArray = readFlowFileToHex(session, flowFile);
            FlowFile outFlowFile = session.create();
            if (outMode.equals("BOTH") || outMode.equals("FLOWFILE")){
                int[] index = getArrayLimits(flowFileOffset, flowFileLength, hexArray.length, 0);
                if(!offsetOutOfBounds(index)) {
                    session.write(outFlowFile,
                        outputStream -> outputStream.write(Arrays.copyOfRange(hexArray, index[0], index[1])));
                    transferFlowFile = true;
                }
            }
            if (outMode.equals("BOTH") || outMode.equals("ATTRIBUTE")){
                int[] index = getArrayLimits(attributeOffset, attributeLength, hexArray.length, 255);
                if(!offsetOutOfBounds(index)) {
                    session.putAttribute(outFlowFile,
                        attributeName,
                        new String(Arrays.copyOfRange(hexArray, index[0], index[1] ), StandardCharsets.UTF_8));
                    transferFlowFile = true;
                }
            }

            if (transferFlowFile){
                session.transfer(outFlowFile, REL_SUCCESS);
            }
            session.remove(flowFile);
        } catch (ProcessException | IOException e) {
            getLogger().error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}

package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "attributes"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates the attributes from a FlowFile and generates a JSON string with the attribute key/value pair. " +
        "The resulting JSON string is placed in the FlowFile as a new Attribute with the name 'JSONAttributes'. By default all" +
        "Attributes in the FlowFile are placed in the resulting JSON string. If only certain Attributes are desired you may" +
        "specify a list of FlowFile Attributes that you want in the resulting JSON string")
@WritesAttribute(attribute = "JSONAttributes", description = "JSON string of all the pre-existing attributes in the flowfile")
public class AttributesToJSON extends AbstractProcessor {

    public static final String JSON_ATTRIBUTE_NAME = "JSONAttribute";
    private static final String AT_LIST_SEPARATOR = ",";

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";


    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the '" + JSON_ATTRIBUTE_NAME +"' " +
                    "attribute. This list of attributes is case sensitive. If a specified attribute is not found" +
                    "in the flowfile an empty string will be output for that attritbute in the resulting JSON")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Control if JSON value is written as a new flowfile attribute '" + JSON_ATTRIBUTE_NAME + "' " +
                    "or written in the flowfile content. " +
                    "Writing to flowfile content will overwrite any existing flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes which are " +
                    "contained in every FlowFile should be included in the final JSON value generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name("Null Value")
            .description("If an Attribute is value is empty or not present this property determines if an empty string" +
                    "or true NULL value is present in the resulting JSON output")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("'" + JSON_ATTRIBUTE_NAME + "' attribute has been successfully added to the flowfile").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to add '" + JSON_ATTRIBUTE_NAME + "' attribute to the flowfile").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTES_LIST);
        properties.add(DESTINATION);
        properties.add(INCLUDE_CORE_ATTRIBUTES);
        properties.add(NULL_VALUE_FOR_EMPTY_STRING);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    /**
     * Builds the Map of attributes that should be included in the JSON that is emitted from this process.
     *
     * @return
     *  Map<String, String> of values that are feed to a Jackson ObjectMapper
     */
    protected Map<String, String> buildAttributesMapForFlowFile(FlowFile ff, String atrList,
                                                                boolean includeCoreAttributes,
                                                                boolean nullValForEmptyString) {

        Map<String, String> atsToWrite = new HashMap<>();

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, val);
                    } else {
                        if (nullValForEmptyString) {
                            atsToWrite.put(cleanStr, null);
                        } else {
                            atsToWrite.put(cleanStr, "");
                        }
                    }
                }
            }
        } else {
            atsToWrite.putAll(ff.getAttributes());
        }

        if (!includeCoreAttributes) {
            atsToWrite = removeCoreAttributes(atsToWrite);
        }

        return atsToWrite;
    }

    /**
     * Remove all of the CoreAttributes from the Attributes that will be written to the Flowfile.
     *
     * @param atsToWrite
     *  List of Attributes that have already been generated including the CoreAttributes
     *
     * @return
     *  Difference of all attributes minus the CoreAttributes
     */
    protected Map<String, String> removeCoreAttributes(Map<String, String> atsToWrite) {
        for (CoreAttributes c : CoreAttributes.values()) {
            atsToWrite.remove(c.key());
        }
        return atsToWrite;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final Map<String, String> atrList = buildAttributesMapForFlowFile(original,
                context.getProperty(ATTRIBUTES_LIST).getValue(),
                context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean(),
                context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean());

        try {

            switch (context.getProperty(DESTINATION).getValue()) {
                case DESTINATION_ATTRIBUTE:
                    FlowFile atFlowfile = session.putAttribute(original, JSON_ATTRIBUTE_NAME,
                            objectMapper.writeValueAsString(atrList));
                    session.transfer(atFlowfile, REL_SUCCESS);
                    break;
                case DESTINATION_CONTENT:
                    FlowFile conFlowfile = session.write(original, new StreamCallback() {
                        @Override
                        public void process(InputStream in, OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(objectMapper.writeValueAsBytes(atrList));
                            }
                        }
                    });
                    session.transfer(conFlowfile, REL_SUCCESS);
                    break;
            }

        } catch (JsonProcessingException e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}

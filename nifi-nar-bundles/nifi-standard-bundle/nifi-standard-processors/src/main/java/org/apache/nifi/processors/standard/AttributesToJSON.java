package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"JSON", "attributes"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates the attributes from a FlowFile and generates a JSON string with the attribute key/value pair. " +
        "The resulting JSON string is placed in the FlowFile as a new Attribute with the name 'JSONAttributes'. By default all" +
        "Attributes in the FlowFile are placed in the resulting JSON string. If only certain Attributes are desired you may" +
        "specify a list of FlowFile Attributes that you want in the resulting JSON string")
@WritesAttribute(attribute = "JSONAttributes", description = "JSON string of all the pre-existing attributes in the flowfile")
public class AttributesToJSON extends AbstractProcessor {

    public static final String JSON_ATTRIBUTE_NAME = "JSONAttribute";
    private static final String AT_LIST_SEPARATOR = ",";
    private static final String DEFAULT_VALUE_IF_ATT_NOT_PRESENT = "";

    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the '" + JSON_ATTRIBUTE_NAME +"' attribute. This list of attributes is case sensitive. If a specified attribute is not found" +
                    "in the flowfile an empty string will be output for that attritbute in the resulting JSON")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final String atList = context.getProperty(ATTRIBUTES_LIST).getValue();
        Map<String, String> atsToWrite = null;

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (atList != null && !StringUtils.isEmpty(atList)) {
            atsToWrite = new HashMap<>();
            String[] ats = StringUtils.split(atList, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = original.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, val);
                    } else {
                        atsToWrite.put(cleanStr, DEFAULT_VALUE_IF_ATT_NOT_PRESENT);
                    }
                }
            }
        } else {
            atsToWrite = original.getAttributes();
        }

        if (atsToWrite != null) {
            if (atsToWrite.size() == 0) {
                getLogger().debug("Flowfile contains no attributes to convert to JSON");
            } else {
                try {
                    FlowFile updated = session.putAttribute(original, JSON_ATTRIBUTE_NAME, objectMapper.writeValueAsString(atsToWrite));
                    session.transfer(updated, REL_SUCCESS);
                } catch (JsonProcessingException e) {
                    getLogger().error(e.getMessage());
                    session.transfer(original, REL_FAILURE);
                }
            }
        }

    }
}

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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.JsonFragmentWriter;
import org.apache.nifi.processors.standard.util.JsonStack;
import org.apache.nifi.processors.standard.util.SimpleJsonPath;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParsingException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "split", "jsonpath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits a JSON File into multiple, separate FlowFiles.  Each element of the object or array " +
        "specified by the JSON Path will be sent in a dedicated flowfile to the 'split' relationship.  The " +
        "original file will be transferred to the 'original' relationship.  If the specified JSON Path is not found, " +
        "the original file is routed to 'failure' and no files are generated. "+
        "The purpose of this processor is to minimize the amount of memory needed to split a JSON document.  It is " +
        "intended for use with large JSON files when the JSON Path is rudimentary. This processor conserves memory " +
        "by reading documents in a streaming fashion (without loading the whole document into memory at once) and " +
        "therefore cannot support all JSON Path expressions.  Note, however, that a fragment.index attribute is set " +
        "on every outgoing FlowFile. This, in combination with a RouteOnAttribute processor, can be used in place " +
        "of certain JSON Path constructs. The specified JSON Path will be checked and the processor will reflect " +
        "an invalid state if the expression is not supported.  Lastly, in contrast with the SplitJson processor, " +
        "every generated flowfile will always be a self-contained valid JSON document.  For example, when " +
        "splitting an array of scalar values, each resulting document will be framed in array context.")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All split FlowFiles produced from the same parent FlowFile will have the same " +
                        "randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the split FlowFiles that were " +
                        "created from a single parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@SeeAlso(SplitJson.class)
public class SplitLargeJson extends AbstractProcessor {

    public static final PropertyDescriptor JSON_PATH_EXPRESSION = new PropertyDescriptor.Builder()
            .name("JsonPath Expression")
            .description("A JsonPath expression that indicates the array element to split into JSON/scalar fragments.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR) // Full validation occurs in #customValidate
            .required(true)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, " +
                    "nothing will be sent to this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid " +
                    "JSON or the specified path does not exist), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(JSON_PATH_EXPRESSION);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SPLIT);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
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
        String value = validationContext.getProperty(JSON_PATH_EXPRESSION).getValue();
        ValidationResult.Builder vrb = new ValidationResult.Builder();
        try {
            SimpleJsonPath.of(value);
            vrb.valid(true);
        } catch (Exception e) {
            vrb.subject(value);
            vrb.input(getClass().getName());
            vrb.explanation("the specified JSON path is either invalid or not supported by this processor.");
        }
        return Collections.singleton(vrb.build());
    }

    /** Provide read-only access to a JsonParser, exposing only the getter methods for the current value. */
    public static class JsonParserView {
        private final JsonParser parser;

        JsonParserView(JsonParser parser) {
            this.parser = parser;
        }

        public String getString() {
            return parser.getString();
        }

        public BigDecimal getBigDecimal() {
            return parser.getBigDecimal();
        }

        public int getInt() {
            return parser.getInt();
        }

        public boolean isIntegralNumber() {
            return parser.isIntegralNumber();
        }
    }

    @Override
    /** The specified JSON Path property will be used to split an incoming FlowFile into multiple FlowFiles.  The
     * method uses the JSON Path property to build a <code>SimpleJsonPath</code> instance.  It then uses the JSR-374
     * API (AKA JSON-P) read the incoming JSON in a streaming fashion, building a <code>JsonStack</code> instance as
     * it goes.  Every time the stack changes it is compared with the <code>SimpleJsonPath</code> to determine if the
     * current point in the file should be copied to an outgoing FlowFile. */
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) {
        FlowFile original = processSession.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        String groupId = UUID.randomUUID().toString();
        SimpleJsonPath path = SimpleJsonPath.of(processContext.getProperty(JSON_PATH_EXPRESSION).getValue());
        JsonFragmentWriter fragmentWriter = new JsonFragmentWriter(processSession, original, REL_SPLIT, groupId,
                path.hasWildcard() ? JsonFragmentWriter.Mode.DISJOINT : JsonFragmentWriter.Mode.CONTIGUOUS);
        JsonStack stack = new JsonStack();
        JsonFragmentWriter.BoolPair section = new JsonFragmentWriter.BoolPair();
        if (path.isEmpty()) {
            section.push(true);
        }

        try (InputStream is = processSession.read(original);
             JsonParser parser = Json.createParser(is)) {
            JsonParserView view = new JsonParserView(parser);
            while (parser.hasNext()) {
                JsonParser.Event e = parser.next();
                stack.receive(e, view);
                section.push(stack.startsWith(path));
                fragmentWriter.filterEvent(section, e, view, stack.size());
            }
        } catch (JsonParsingException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{original});
            processSession.transfer(original, REL_FAILURE);
            return;
        } catch (Exception e) {
            logger.error("Problems with FlowFile {}: {}", new Object[]{original, e.getMessage()});
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        if (fragmentWriter.getCount() == 0) {
            logger.error("No object or array was found at the specified json path");
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        logger.info("Split {} into {} FlowFile(s)", new Object[]{original, fragmentWriter.getCount()});
        original = copyAttributesToOriginal(processSession, original, groupId, fragmentWriter.getCount());
        processSession.transfer(original, REL_ORIGINAL);
    }
}

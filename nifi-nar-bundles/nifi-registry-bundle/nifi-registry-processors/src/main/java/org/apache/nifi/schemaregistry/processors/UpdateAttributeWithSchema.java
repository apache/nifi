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
package org.apache.nifi.schemaregistry.processors;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

@Tags({"registry", "schema"})
@CapabilityDescription("Extracts the schema for the content of the incoming FlowFile using the provided Schema Registry Service.")
@WritesAttributes({@WritesAttribute(attribute="schema.text", description="Textual representation of the schema retrieved from the Schema Registry")})
public final class UpdateAttributeWithSchema extends AbstractTransformer {

    /**
     *
     */
    @Override
    protected Map<String, String> transform(InputStream in, InvocationContextProperties contextProperties, Schema schema) {
        return Collections.unmodifiableMap(Collections.singletonMap(SCHEMA_ATTRIBUTE_NAME, schema.toString()));
    }
}

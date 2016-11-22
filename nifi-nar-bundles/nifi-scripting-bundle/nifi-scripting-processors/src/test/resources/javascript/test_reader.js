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

/*
var processor = new org.apache.nifi.processor.Processor() {
This will cause following error:
'ScriptValidation' validated against 'target/test/resources/javascript/test_reader.js' is invalid because Unable to load script due to getInterface cannot be called on non-script object
*/

var String = Java.type("java.lang.String");
var Set = Java.type("java.util.HashSet");
var Relationship = Java.type("org.apache.nifi.processor.Relationship");
var PropertyDescriptor = Java.type("org.apache.nifi.components.PropertyDescriptor");
var StandardValidators = Java.type("org.apache.nifi.processor.util.StandardValidators");
var StreamUtils = Java.type("org.apache.nifi.stream.io.StreamUtils");

var REL_TEST = new Relationship.Builder()
        .name("test")
        .description("A test relationship")
        .build();

var PROP_TEST_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("test-attribute").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build()

var propertyDescriptors = [PROP_TEST_ATTRIBUTE];

var processor = new Object() {
    logger: null,

    // Processor
    initialize: function(context) {
    },

    getRelationships: function() {
        var relationships = new Set();
        relationships.addAll([REL_TEST]);
        return relationships;
    },

    onTrigger: function(context, sessionFactory) {
        var session = sessionFactory.createSession()
        var flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Set local logger variable so that read callback can use it.
        var logger = this.logger;

        // Read content from flowFile using Java classes.
        var content;
        session.read(flowFile, function (inputStream) {
            var buffer = Java.to(new Array(flowFile.getSize()), "byte[]");
            logger.info("Reading from inputStream, size={}, {}", [flowFile.getSize(), inputStream]);
            StreamUtils.fillBuffer(inputStream, buffer, false);
            content = new String(buffer, "UTF-8");
        })

        this.logger.info("Read content={}", [content]);

        flowFile = session.putAttribute(flowFile, "from-content", content)
        // transfer
        session.transfer(flowFile, REL_TEST)
        session.commit()
    },

    // ConfigurableComponent
    validate: function(context) {
        return [];
    },

    getPropertyDescriptor: function(name) {
        for (var i = 0; i < propertyDescriptors.length; i++) {
            if (name === p.getName) return p;
        }
        return null;
    },

    onPropertyModified: function(descriptor, oldValue, newValue) {
    },

    getPropertyDescriptors: function() {
        return propertyDescriptors;
    },

    getIdentifier: function() {
        return null;
    },

    setLogger: function(logger) {
        this.logger = logger;
        this.logger.info("logger is set! logger={}", [logger]);
    }
};

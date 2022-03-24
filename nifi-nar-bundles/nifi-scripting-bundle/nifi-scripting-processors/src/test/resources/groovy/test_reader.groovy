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
class GroovyProcessor implements Processor {

    def REL_TEST = new Relationship.Builder()
            .name("test")
            .description("A test relationship")
            .build();

    def descriptor = new PropertyDescriptor.Builder()
            .name("test-attribute").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build()

    def logger

    def setAttributeFromThisInOnScheduled = ''

    @Override
    void initialize(ProcessorInitializationContext context) {

    }

    void setLogger(log) {
        logger = log
    }

    void onScheduled(ProcessContext context) {
        // Set the attribute value for use in onTrigger
        setAttributeFromThisInOnScheduled = 'test content'

        // Try to parse a date here, will fail after Groovy 2.5.0 if groovy-dateutil is not included
        Date.parse('yyyyMMdd', '20190630')
    }

    void onStopped(ProcessContext context) {
        logger.info("Called onStopped")
    }

    @Override
    Set<Relationship> getRelationships() {
        return [REL_TEST] as Set
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        def session = sessionFactory.createSession()
        def flowFile = session.get()
        if (flowFile == null) {
            return
        }
        flowFile = session.putAttribute(flowFile, 'from-content', setAttributeFromThisInOnScheduled)
        // transfer
        session.transfer(flowFile, REL_TEST)
        session.commitAsync()
    }

    @Override
    Collection<ValidationResult> validate(ValidationContext context) {
        return null
    }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        return (name?.equals("test-attribute") ? descriptor : null)
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

    }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {

        return [descriptor] as List
    }

    @Override
    String getIdentifier() {
        return null
    }
}

processor = new GroovyProcessor()
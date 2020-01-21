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

import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSessionFactory
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.exception.ProcessException

class DynamicSensitivePropertyGroovyProcessor implements Processor {
    ComponentLog log

    def REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles that were successfully processed").build()

    @Override
    Set<Relationship> getRelationships() {
        return [REL_SUCCESS] as Set
    }

    def PASS = new PropertyDescriptor.Builder().name('Password')
            .required(true).sensitive(true).addValidator(Validator.VALID).build()

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        log.info("Adding password property descriptor: ${PASS}")
        return [PASS] as List
    }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        switch(name){
            case 'Password': return PASS
            default: return null
        }
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        try {
            def session = sessionFactory.createSession()

            def flowFile = session.get()
            if(!flowFile) return

            def pass = context.getProperty(PASS).getValue()
            // do something with pass

            session.transfer(flowFile,REL_SUCCESS)
            session.commit()
        }
        catch (e) {
            throw new ProcessException(e)
        }
    }

    @Override
    void initialize(ProcessorInitializationContext context) {
        log = context.getLogger()
    }

    @Override
    Collection<ValidationResult> validate(ValidationContext context) { return null }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) { }

    @Override
    String getIdentifier() { return null }
}

processor = new DynamicSensitivePropertyGroovyProcessor()
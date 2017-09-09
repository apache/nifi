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
import org.apache.commons.codec.binary.Hex

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.stream.io.StreamUtils

import java.security.MessageDigest


class TestAbstractProcessor extends AbstractProcessor {

	def REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("FlowFiles that were successfully processed")
			.build();

	final static String attr = "outAttr";

	final static PropertyDescriptor myCustomProp = new PropertyDescriptor.Builder()
			.name("custom_prop")
			.displayName("a custom prop")
			.description("bla bla bla")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	@Override
	List<PropertyDescriptor> getSupportedPropertyDescriptors(){
		return [myCustomProp] as List
	}

	@Override
	Set<Relationship> getRelationships() {
		return [REL_SUCCESS] as Set
	}

	@Override
	Collection<ValidationResult> customValidate(ValidationContext context) {
		final String myCustomPropValue = context.getProperty(myCustomProp).getValue()
		if (myCustomPropValue.length() < 3) {
			final List<ValidationResult> problems = new ArrayList<>(1);
			problems.add(new ValidationResult.Builder().subject(myCustomProp.getName())
					.input(myCustomPropValue)
					.valid(false)
					.explanation("myCustomProp should contain a string with at least 3 chars.")
					.build());
			return problems;
		}
		return super.customValidate(context)
	}

	@Override
	void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile requestFlowFile = session.get();
		String myCustomPropValue = context.getProperty(myCustomProp).evaluateAttributeExpressions(requestFlowFile).getValue();
		final byte[] buff = new byte[(int) requestFlowFile.getSize()];
		session.read(requestFlowFile, new InputStreamCallback() {
			@Override
			void process(InputStream inp) throws IOException {
				StreamUtils.fillBuffer(inp, buff);
			}
		});
		final String content = new String(buff);
		final String md5 = generateMD5_A(content + myCustomPropValue);
		requestFlowFile = session.putAttribute(requestFlowFile, attr, md5);
		requestFlowFile = session.write(requestFlowFile, new OutputStreamCallback() {
			@Override
			public void process(OutputStream out) throws IOException {
				out.write(md5.getBytes());
			}
		});

		session.transfer(requestFlowFile, REL_SUCCESS);
	}

    static def generateMD5_A(String s){
        new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(s.bytes)));
	}
}

processor = new TestAbstractProcessor();

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import re
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, PropertyDependency, ExpressionLanguageScope, TimeUnit


FLOWFILE_CONTENT = 'flowfile_content'
FLOWFILE_CONTENT_REFERENCE = '{' + FLOWFILE_CONTENT + '}'
# Regex to match { followed by any number of characters other than { or }, followed by }. But do not match if it starts with {{
VAR_NAME_REGEX = r'(?<!{)\{([^{]*?)\}'


class PromptChatGPT(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '@project.version@'
        description = "Submits a prompt to ChatGPT, writing the results either to a FlowFile attribute or to the contents of the FlowFile"
        tags = ["text", "chatgpt", "gpt", "machine learning", "ML", "artificial intelligence", "ai", "document", "langchain"]
        dependencies = ['langchain==0.1.2', 'openai==1.9.0', 'jsonpath-ng']


    MODEL = PropertyDescriptor(
        name="OpenAI Model Name",
        description="The name of the OpenAI Model to use in order to answer the prompt",
        default_value="gpt-3.5-turbo",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=True
    )
    PROMPT = PropertyDescriptor(
        name="Prompt",
        description="The prompt to issue to ChatGPT. This may use FlowFile attributes via Expression Language and may also reference the FlowFile content by using the literal " +
                    "{flowfile_content} (including braces) in the prompt. If the FlowFile's content is JSON formatted, a reference may also include JSONPath Expressions "
                    "to reference specific fields in the FlowFile content, such as {$.page_content}",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )
    TEMPERATURE = PropertyDescriptor(
        name="Temperature",
        description="The Temperature parameter to submit to OpenAI. A lower value will result in more consistent answers while a higher value will result in a more creative answer. " +
                    "The value must be between 0 and 2, inclusive.",
        validators=[StandardValidators._standard_validators.createNonNegativeFloatingPointValidator(2.0)],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="1.0"
    )
    RESULT_ATTRIBUTE = PropertyDescriptor(
        name="Result Attribute",
        description="If specified, the result will be added to the attribute whose name is given. If not specified, the result will be written to the FlowFile's content",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=False
    )
    API_KEY = PropertyDescriptor(
        name="API Key",
        description="The OpenAI API Key to use",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=True,
        sensitive=True
    )
    TIMEOUT = PropertyDescriptor(
        name="Request Timeout",
        description="The amount of time to wait before timing out the request",
        validators=[StandardValidators.TIME_PERIOD_VALIDATOR],
        default_value="60 secs",
        required=True
    )
    MAX_TOKENS = PropertyDescriptor(
        name="Max Tokens to Generate",
        description="The maximum number of tokens that ChatGPT should generate",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        required=False
    )
    ORGANIZATION = PropertyDescriptor(
        name="OpenAI Organization ID",
        description="The OpenAI Organization ID",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=False
    )
    API_BASE = PropertyDescriptor(
        name="API Base URL Path",
        description="The API Base URL to use for interacting with OpenAI. This should be populated only if using a proxy or an emulator.",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=False
    )

    property_descriptors = [
        MODEL,
        PROMPT,
        TEMPERATURE,
        RESULT_ATTRIBUTE,
        API_KEY,
        TIMEOUT,
        MAX_TOKENS,
        ORGANIZATION,
        API_BASE
    ]


    def __init__(self, **kwargs):
        pass


    def getPropertyDescriptors(self):
        return self.property_descriptors


    def transform(self, context, flowFile):
        from langchain import PromptTemplate
        from langchain.chat_models import ChatOpenAI
        from langchain.chains.llm import LLMChain

        prompt = context.getProperty(self.PROMPT).evaluateAttributeExpressions(flowFile).getValue()

        # We want to allow referencing FlowFile content using JSONPath Expressions.
        # To do that, we allow the same {variable} syntax as Langchain. But Langchain does not allow '$' characters
        # to exist in the variable names. So we need to replace those variables in the prompt with new variables, such as
        # jsonpath_var_0, jsonpath_var_1, etc. To do this, we will use a Regex to detect any variables that are referenced
        # and if it starts with a $ we will replace it with jsonpath_var_<index> and we will keep a mapping from that name to
        # the substituted variable name so that we can later determine what the JSONPath expression was.
        variable_references = list(set(re.findall(VAR_NAME_REGEX, prompt)))

        input_variables = []
        jsonpath_to_var_mapping = {}
        index = 0
        for ref in variable_references:
            if ref.startswith("$"):
                var_name = "jsonpath_var_" + str(index)
                index += 1
                input_variables.append(var_name)
                jsonpath_to_var_mapping[ref] = var_name
                prompt = prompt.replace("{" + ref + "}", "{" + var_name + "}")
            elif ref == FLOWFILE_CONTENT:
                input_variables.append(ref)
            else:
                raise ValueError("Prompt contained an invalid variable reference: {" + ref + "}. Valid references are flowfile_content or any JSONPath expression.")

        temperature = context.getProperty(self.TEMPERATURE).evaluateAttributeExpressions(flowFile).asFloat()
        model_name = context.getProperty(self.MODEL).evaluateAttributeExpressions(flowFile).getValue()
        api_key = context.getProperty(self.API_KEY).getValue()
        timeout = context.getProperty(self.TIMEOUT).asTimePeriod(TimeUnit.SECONDS)
        max_tokens = context.getProperty(self.MAX_TOKENS).asInteger()
        organization = context.getProperty(self.ORGANIZATION).getValue()
        api_base = context.getProperty(self.API_BASE).getValue()

        # Build out our LLMChain
        llm = ChatOpenAI(model_name=model_name, temperature=temperature, openai_api_key=api_key, request_timeout=timeout, max_retries=0,
                         max_tokens=max_tokens, openai_organization=organization, openai_api_base=api_base)

        prompt_template = PromptTemplate(
            template=prompt,
            input_variables=input_variables
        )

        llm_chain = LLMChain(
            llm=llm,
            prompt=prompt_template
        )

        # Substitute in any JSON Path Expressions or references to {flowfile_content}.
        llm_args = {}
        json_content = None
        for var_name in variable_references:
            # If variable references {flowfile_content} substitute the content
            if var_name == FLOWFILE_CONTENT:
                llm_args[FLOWFILE_CONTENT] = flowFile.getContentsAsBytes().decode()
            if var_name.startswith("$"):
                # Load the FlowFile's contents into the json_content variable only once
                if json_content is None:
                    json_content = json.loads(flowFile.getContentsAsBytes().decode())

                # Import jsonpath_ng so that we can evaluate JSONPath against the FlowFile content.
                from jsonpath_ng import parse
                try:
                    jsonpath_expression = parse(var_name)
                    matches = jsonpath_expression.find(json_content)
                    variable_value = "\n".join([match.value for match in matches])
                except:
                    self.logger.error("Invalid JSONPath reference in prompt: " + var_name)
                    raise

                # Insert the resolved value into llm_args
                resolved_var_name = jsonpath_to_var_mapping.get(var_name)
                llm_args[resolved_var_name] = variable_value

        self.logger.debug(f"Evaluating prompt\nPrompt: {prompt}\nArgs: #{llm_args}")

        # Run the LLM Chain in order to prompt ChatGPT
        results = llm_chain(llm_args)

        # Create the output content or FLowFile attribute
        text = results['text']
        attribute_name = context.getProperty(self.RESULT_ATTRIBUTE).getValue()
        if attribute_name is None:
            output_content = text
            output_attributes = None
        else:
            output_content = None
            output_attributes = {attribute_name: text}

        # Return the results
        return FlowFileTransformResult("success", contents=output_content, attributes=output_attributes)

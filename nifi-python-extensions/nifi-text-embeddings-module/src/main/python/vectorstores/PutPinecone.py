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

from langchain.vectorstores import Pinecone
from langchain.embeddings.openai import OpenAIEmbeddings
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
import pinecone
import json


class PutPinecone(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.0.0-SNAPSHOT'
        description = """Publishes JSON data to a Pinecone. The Incoming data must be in single JSON per Line format, each with two keys: 'text' and 'metadata'.
                       The text must be a string, while metadata must be a map with strings for values. Any additional fields will be ignored."""
        tags = ["pinecone", "vector", "vectordb", "vectorstore", "embeddings", "ai", "artificial intelligence", "ml", "machine learning", "text", "LLM"]


    PINECONE_API_KEY = PropertyDescriptor(
        name="Pinecone API Key",
        description="The API Key to use in order to authentication with Pinecone",
        sensitive=True,
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR]
    )
    OPENAI_API_KEY = PropertyDescriptor(
        name="OpenAI API Key",
        description="The API Key for OpenAI in order to create embeddings",
        sensitive=True,
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR]
    )
    PINECONE_ENV = PropertyDescriptor(
        name="Pinecone Environment",
        description="The name of the Pinecone Environment. This can be found in the Pinecone console next to the API Key.",
        sensitive=False,
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR]
    )
    INDEX_NAME = PropertyDescriptor(
        name="Index Name",
        description="The name of the Pinecone index.",
        sensitive=False,
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    TEXT_KEY = PropertyDescriptor(
        name="Text Key",
        description="The key in the document that contains the text to create embeddings for.",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="text",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    NAMESPACE = PropertyDescriptor(
        name="Namespace",
        description="The name of the Pinecone Namespace to put the documents to.",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [PINECONE_API_KEY,
                  OPENAI_API_KEY,
                  PINECONE_ENV,
                  INDEX_NAME,
                  TEXT_KEY,
                  NAMESPACE]

    embeddings = None

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def onScheduled(self, context):
        api_key = context.getProperty(self.PINECONE_API_KEY).getValue()
        pinecone_env = context.getProperty(self.PINECONE_ENV).getValue()

        # initialize pinecone
        pinecone.init(
            api_key=api_key,
            environment=pinecone_env,
        )
        openai_api_key = context.getProperty(self.OPENAI_API_KEY).getValue()
        self.embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)


    def transform(self, context, flowfile):
        # First, check if our index already exists. If it doesn't, we create it
        index_name = context.getProperty(self.INDEX_NAME).evaluateAttributeExpressions(flowfile).getValue()
        namespace = context.getProperty(self.NAMESPACE).evaluateAttributeExpressions(flowfile).getValue()

        index = pinecone.Index(index_name)

        # Read the FlowFile content as "json-lines".
        json_lines = flowfile.getContentsAsBytes().decode()
        i = 0
        texts = []
        metadatas = []
        ids = []
        for line in json_lines.split("\n"):
            doc = json.loads(line)
            text = doc.get('text')
            metadata = doc.get('metadata')
            texts.append(text)

            # Remove any null values, or it will cause the embedding to fail
            filtered_metadata = {}
            for key, value in metadata.items():
                if value is not None:
                    filtered_metadata[key] = value

            metadatas.append(filtered_metadata)
            ids.append(flowfile.getAttribute("uuid") + "-" + str(i))
            i += 1

        text_key = context.getProperty(self.TEXT_KEY).evaluateAttributeExpressions().getValue()
        vectorstore = Pinecone(index, self.embeddings.embed_query, text_key)
        vectorstore.add_texts(texts=texts, metadatas=metadatas, ids=ids, namespace=namespace)
        return FlowFileTransformResult(relationship = "success")

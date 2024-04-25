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

import langchain.vectorstores
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope, PropertyDependency
from pinecone import Pinecone
import json
from EmbeddingUtils import OPENAI, HUGGING_FACE, EMBEDDING_MODEL, create_embedding_service
from nifiapi.documentation import use_case

@use_case(description="Create vectors/embeddings that represent text content and send the vectors to Pinecone",
          notes="This use case assumes that the data has already been formatted in JSONL format with the text to store in Pinecone provided in the 'text' field.",
          keywords=["pinecone", "embedding", "vector", "text", "vectorstore", "insert"],
          configuration="""
                Configure the 'Pinecone API Key' to the appropriate authentication token for interacting with Pinecone.
                Configure 'Embedding Model' to indicate whether OpenAI embeddings should be used or a HuggingFace embedding model should be used: 'Hugging Face Model' or 'OpenAI Model'
                Configure the 'OpenAI API Key' or 'HuggingFace API Key', depending on the chosen Embedding Model.
                Set 'Pinecone Environment' to the name of your Pinecone environment
                Set 'Index Name' to the name of your Pinecone Index.
                Set 'Namespace' to appropriate namespace, or leave it empty to use the default Namespace.

                If the documents to send to Pinecone contain a unique identifier, set the 'Document ID Field Name' property to the name of the field that contains the document ID.
                This property can be left blank, in which case a unique ID will be generated based on the FlowFile's filename.
                """)
@use_case(description="Update vectors/embeddings in Pinecone",
          notes="This use case assumes that the data has already been formatted in JSONL format with the text to store in Pinecone provided in the 'text' field.",
          keywords=["pinecone", "embedding", "vector", "text", "vectorstore", "update", "upsert"],
          configuration="""
                Configure the 'Pinecone API Key' to the appropriate authentication token for interacting with Pinecone.
                Configure 'Embedding Model' to indicate whether OpenAI embeddings should be used or a HuggingFace embedding model should be used: 'Hugging Face Model' or 'OpenAI Model'
                Configure the 'OpenAI API Key' or 'HuggingFace API Key', depending on the chosen Embedding Model.
                Set 'Pinecone Environment' to the name of your Pinecone environment
                Set 'Index Name' to the name of your Pinecone Index.
                Set 'Namespace' to appropriate namespace, or leave it empty to use the default Namespace.
                Set the 'Document ID Field Name' property to the name of the field that contains the identifier of the document in Pinecone to update.
                """)
class PutPinecone(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '@project.version@'
        description = """Publishes JSON data to Pinecone. The Incoming data must be in single JSON per Line format, each with two keys: 'text' and 'metadata'.
                       The text must be a string, while metadata must be a map with strings for values. Any additional fields will be ignored."""
        tags = ["pinecone", "vector", "vectordb", "vectorstore", "embeddings", "ai", "artificial intelligence", "ml", "machine learning", "text", "LLM"]


    PINECONE_API_KEY = PropertyDescriptor(
        name="Pinecone API Key",
        description="The API Key to use in order to authentication with Pinecone",
        sensitive=True,
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR]
    )
    HUGGING_FACE_API_KEY = PropertyDescriptor(
        name="HuggingFace API Key",
        description="The API Key for interacting with HuggingFace",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=True,
        sensitive=True,
        dependencies=[PropertyDependency(EMBEDDING_MODEL, HUGGING_FACE)]
    )
    HUGGING_FACE_MODEL = PropertyDescriptor(
        name="HuggingFace Model",
        description="The name of the HuggingFace model to use",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=True,
        default_value="sentence-transformers/all-MiniLM-L6-v2",
        dependencies=[PropertyDependency(EMBEDDING_MODEL, HUGGING_FACE)]
    )
    OPENAI_API_KEY = PropertyDescriptor(
        name="OpenAI API Key",
        description="The API Key for OpenAI in order to create embeddings",
        sensitive=True,
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        dependencies=[PropertyDependency(EMBEDDING_MODEL, OPENAI)]
    )
    OPENAI_API_MODEL = PropertyDescriptor(
        name="OpenAI Model",
        description="The API Key for OpenAI in order to create embeddings",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="text-embedding-ada-002",
        dependencies=[PropertyDependency(EMBEDDING_MODEL, OPENAI)]
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
    DOC_ID_FIELD_NAME = PropertyDescriptor(
        name="Document ID Field Name",
        description="""Specifies the name of the field in the 'metadata' element of each document where the document's ID can be found.  
                    If not specified, an ID will be generated based on the FlowFile's filename and a one-up number.""",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [PINECONE_API_KEY,
                  EMBEDDING_MODEL,
                  OPENAI_API_KEY,
                  OPENAI_API_MODEL,
                  HUGGING_FACE_API_KEY,
                  HUGGING_FACE_MODEL,
                  PINECONE_ENV,
                  INDEX_NAME,
                  TEXT_KEY,
                  NAMESPACE,
                  DOC_ID_FIELD_NAME]

    embeddings = None
    pc = None

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def onScheduled(self, context):
        # initialize pinecone
        self.pc = Pinecone(
            api_key=context.getProperty(self.PINECONE_API_KEY).getValue(),
            environment=context.getProperty(self.PINECONE_ENV).getValue()
        )
        # initialize embedding service
        self.embeddings = create_embedding_service(context)

    def transform(self, context, flowfile):
        # First, check if our index already exists. If it doesn't, we create it
        index_name = context.getProperty(self.INDEX_NAME).evaluateAttributeExpressions(flowfile).getValue()
        namespace = context.getProperty(self.NAMESPACE).evaluateAttributeExpressions(flowfile).getValue()
        id_field_name = context.getProperty(self.DOC_ID_FIELD_NAME).evaluateAttributeExpressions(flowfile).getValue()

        index = self.pc.Index(index_name)

        # Read the FlowFile content as "json-lines".
        json_lines = flowfile.getContentsAsBytes().decode()
        i = 1
        texts = []
        metadatas = []
        ids = []
        for line in json_lines.split("\n"):
            try:
                doc = json.loads(line)
            except Exception as e:
                raise ValueError(f"Could not parse line {i} as JSON") from e

            text = doc.get('text')
            metadata = doc.get('metadata')
            texts.append(text)

            # Remove any null values, or it will cause the embedding to fail
            filtered_metadata = {}
            for key, value in metadata.items():
                if value is not None:
                    filtered_metadata[key] = value

            metadatas.append(filtered_metadata)

            doc_id = None
            if id_field_name is not None:
                doc_id = metadata.get(id_field_name)
            if doc_id is None:
                doc_id = flowfile.getAttribute("filename") + "-" + str(i)
            ids.append(doc_id)

            i += 1

        text_key = context.getProperty(self.TEXT_KEY).evaluateAttributeExpressions().getValue()
        vectorstore = langchain.vectorstores.Pinecone(index, self.embeddings.embed_query, text_key)
        vectorstore.add_texts(texts=texts, metadatas=metadatas, ids=ids, namespace=namespace)
        return FlowFileTransformResult(relationship="success")

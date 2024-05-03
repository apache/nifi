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

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
import ChromaUtils
import EmbeddingUtils


class PutChroma(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '@project.version@'
        description = """Publishes JSON data to a Chroma VectorDB. The Incoming data must be in single JSON per Line format, each with two keys: 'text' and 'metadata'.
                       The text must be a string, while metadata must be a map with strings for values. Any additional fields will be ignored. If the collection name specified
                       does not exist, the Processor will automatically create the collection."""
        tags = ["chroma", "vector", "vectordb", "embeddings", "ai", "artificial intelligence", "ml", "machine learning", "text", "LLM"]


    STORE_TEXT = PropertyDescriptor(
        name="Store Document Text",
        description="Specifies whether or not the text of the document should be stored in Chroma. If so, both the document's text and its embedding will be stored. If not, " +
                    "only the vector/embedding will be stored.",
        allowable_values=["true", "false"],
        required=True,
        default_value="true"
    )
    DISTANCE_METHOD = PropertyDescriptor(
        name="Distance Method",
        description="If the specified collection does not exist, it will be created using this Distance Method. If the collection exists, this property will be ignored.",
        allowable_values=["cosine", "l2", "ip"],
        default_value="cosine",
        required=True
    )
    DOC_ID_FIELD_NAME = PropertyDescriptor(
        name="Document ID Field Name",
        description="Specifies the name of the field in the 'metadata' element of each document where the document's ID can be found. " +
                    "If not specified, an ID will be generated based on the FlowFile's filename and a one-up number.",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )


    client = None
    embedding_function = None

    def __init__(self, **kwargs):
        self.property_descriptors = [prop for prop in ChromaUtils.PROPERTIES] + [prop for prop in EmbeddingUtils.PROPERTIES]
        self.property_descriptors.append(self.STORE_TEXT)
        self.property_descriptors.append(self.DISTANCE_METHOD)
        self.property_descriptors.append(self.DOC_ID_FIELD_NAME)


    def getPropertyDescriptors(self):
        return self.property_descriptors

    def onScheduled(self, context):
        self.client = ChromaUtils.create_client(context)
        self.embedding_function = EmbeddingUtils.create_embedding_function(context)


    def transform(self, context, flowfile):
        client = self.client
        embedding_function = self.embedding_function
        collection_name = context.getProperty(ChromaUtils.COLLECTION_NAME).evaluateAttributeExpressions(flowfile).getValue()
        distance_method = context.getProperty(self.DISTANCE_METHOD).getValue()
        id_field_name = context.getProperty(self.DOC_ID_FIELD_NAME).evaluateAttributeExpressions(flowfile).getValue()

        collection = client.get_or_create_collection(
            name=collection_name,
            embedding_function=embedding_function,
            metadata={"hnsw:space": distance_method})

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

            doc_id = None
            if id_field_name is not None:
                doc_id = metadata.get(id_field_name)
            if doc_id is None:
                doc_id = flowfile.getAttribute("filename") + "-" + str(i)
            ids.append(doc_id)

            i += 1

        embeddings = embedding_function(texts)
        if not context.getProperty(self.STORE_TEXT).asBoolean():
            texts = None

        collection.upsert(ids, embeddings, metadatas, texts)

        return FlowFileTransformResult(relationship = "success")

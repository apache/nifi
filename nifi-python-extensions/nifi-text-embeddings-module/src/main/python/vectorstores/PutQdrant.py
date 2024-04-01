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

from langchain.vectorstores.qdrant import Qdrant
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import (
    PropertyDescriptor,
    StandardValidators,
    ExpressionLanguageScope,
)
import json
from EmbeddingUtils import (
    create_embedding_service,
)
from nifiapi.documentation import use_case

from qdrant_client.models import Distance

import QdrantUtils


@use_case(
    description="Create embeddings that semantically represent text content and upload to Qdrant - https://qdrant.tech/",
    notes="This processor assumes that the data has already been formatted in JSONL format with the text to store in Qdrant provided in the 'text' field.",
    keywords=["qdrant", "embedding", "vector", "text", "vectorstore", "insert"],
    configuration="""
                Configure 'Collection Name' to the name of the Qdrant collection to use.
                Configure 'Qdrant URL' to the fully qualified URL of the Qdrant instance.
                Configure 'Qdrant API Key' to the API Key to use in order to authenticate with Qdrant.
                Configure 'Prefer gRPC' to True if you want to use gRPC for interfacing with Qdrant.
                Configure 'Use HTTPS' to True if you want to use TLS(HTTPS) while interfacing with Qdrant.
                Configure 'Embedding Model' to indicate whether OpenAI embeddings should be used or a HuggingFace embedding model should be used: 'Hugging Face Model' or 'OpenAI Model'
                Configure 'HuggingFace API Key' or 'OpenAI API Key', depending on the chosen Embedding Model.
                Configure 'HuggingFace Model' or 'OpenAI Model' to the name of the model to use.
                Configure 'Force Recreate Collection' to True if you want to recreate the collection if it already exists.
                Configure 'Similarity Metric' to the similarity metric to use when querying Qdrant.

                If the documents to send to Qdrant contain a unique identifier(UUID), set the 'Document ID Field Name' property to the name of the field that contains the document ID.
                This property can be left blank, in which case a UUID will be generated based on the FlowFile's filename.
                """,
)
class PutQdrant(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "@project.version@"
        description = """Publishes JSON data to Qdrant. The Incoming data must be in single JSON per Line format, each with two keys: 'text' and 'metadata'.
                       The text must be a string, while metadata must be a map with strings for values. Any additional fields will be ignored."""
        tags = [
            "qdrant",
            "vector",
            "vectordb",
            "vectorstore",
            "embeddings",
            "ai",
            "artificial intelligence",
            "ml",
            "machine learning",
            "text",
            "LLM",
        ]

    DOC_ID_FIELD_NAME = PropertyDescriptor(
        name="Document ID Field Name",
        description="""Specifies the name of the field in the 'metadata' element of each document where the document's ID can be found.  
                    If not specified, a UUID will be generated based on the FlowFile's filename and an incremental number.""",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )
    FORCE_RECREATE_COLLECTION = PropertyDescriptor(
        name="Force Recreate Collection",
        description="Specifies whether to recreate the collection if it already exists. Essentially clearing the existing data.",
        required=True,
        default_value=False,
        allowable_values=["True", "False"],
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
    )
    SIMILARITY_METRIC = PropertyDescriptor(
        name="Similarity Metric",
        description="Specifies the similarity metric when creating the collection.",
        required=True,
        default_value=Distance.COSINE,
        allowable_values=[
            Distance.COSINE,
            Distance.EUCLID,
            Distance.DOT,
            Distance.MANHATTAN,
        ],
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )

    properties = (
        QdrantUtils.QDRANT_PROPERTIES
        + QdrantUtils.EMBEDDING_MODEL_PROPERTIES
        + [
            FORCE_RECREATE_COLLECTION,
            SIMILARITY_METRIC,
            DOC_ID_FIELD_NAME,
        ]
    )

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def onScheduled(self, context):
        # The Qdrant#construct_instance() internally checks if the collection exists
        # and creates it if it doesn't with the appropriate dimesions and configurations.
        self.vector_store = Qdrant.construct_instance(
            texts=[
                "Some text to obtain the embeddings dimension when creating the collection"
            ],
            embedding=create_embedding_service(context),
            collection_name=context.getProperty(QdrantUtils.COLLECTION_NAME).getValue(),
            url=context.getProperty(QdrantUtils.QDRANT_URL).getValue(),
            api_key=context.getProperty(QdrantUtils.QDRANT_API_KEY).getValue(),
            prefer_grpc=context.getProperty(QdrantUtils.PREFER_GRPC).asBoolean(),
            https=context.getProperty(QdrantUtils.HTTPS).asBoolean(),
            force_recreate=context.getProperty(
                self.FORCE_RECREATE_COLLECTION
            ).asBoolean(),
            distance_func=context.getProperty(self.SIMILARITY_METRIC).getValue(),
        )

    def transform(self, context, flowfile):
        id_field_name = (
            context.getProperty(self.DOC_ID_FIELD_NAME)
            .evaluateAttributeExpressions(flowfile)
            .getValue()
        )

        # Read the FlowFile content as "json-lines".
        json_lines = flowfile.getContentsAsBytes().decode()
        i = 1
        texts, metadatas, ids = [], [], []
        for line in json_lines.split("\n"):
            try:
                doc = json.loads(line)
            except Exception as e:
                raise ValueError(f"Could not parse line {i} as JSON") from e

            metadata = doc.get("metadata")
            texts.append(doc.get("text"))
            metadatas.append(metadata)

            doc_id = None
            if id_field_name is not None:
                doc_id = metadata.get(id_field_name)
            if doc_id is None:
                doc_id = QdrantUtils.convert_id(
                    flowfile.getAttribute("filename") + "-" + str(i)
                )
            ids.append(doc_id)

            i += 1

        self.vector_store.add_texts(texts=texts, metadatas=metadatas, ids=ids)
        return FlowFileTransformResult(relationship="success")

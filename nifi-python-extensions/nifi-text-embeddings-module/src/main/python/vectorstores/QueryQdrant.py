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
import QueryUtils
import json
from EmbeddingUtils import (
    create_embedding_service,
)

from nifiapi.documentation import use_case

from qdrant_client import QdrantClient

import QdrantUtils


@use_case(
    description="Semantically search for documents stored in Qdrant - https://qdrant.tech/",
    keywords=["qdrant", "embedding", "vector", "text", "vectorstore", "search"],
    configuration="""
                Configure 'Collection Name' to the name of the Qdrant collection to use.
                Configure 'Qdrant URL' to the fully qualified URL of the Qdrant instance.
                Configure 'Qdrant API Key' to the API Key to use in order to authenticate with Qdrant.
                Configure 'Prefer gRPC' to True if you want to use gRPC for interfacing with Qdrant.
                Configure 'Use HTTPS' to True if you want to use TLS(HTTPS) while interfacing with Qdrant.
                Configure 'Embedding Model' to indicate whether OpenAI embeddings should be used or a HuggingFace embedding model should be used: 'Hugging Face Model' or 'OpenAI Model'
                Configure 'HuggingFace API Key' or 'OpenAI API Key', depending on the chosen Embedding Model.
                Configure 'HuggingFace Model' or 'OpenAI Model' to the name of the model to use.
                Configure 'Query' to the text of the query to send to Qdrant.
                Configure 'Number of Results' to the number of results to return from Qdrant.
                Configure 'Metadata Filter' to apply an optional metadata filter with the query. For example: { "author": "john.doe" }
                Configure 'Output Strategy' to indicate how the output should be formatted: 'Row-Oriented', 'Text', or 'Column-Oriented'.
                Configure 'Results Field' to the name of the field to insert the results, if the input FlowFile is JSON Formatted,.
                Configure 'Include Metadatas' to True if metadata should be included in the output.
                Configure 'Include Distances' to True if distances should be included in the output.
                """,
)
class QueryQdrant(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "@project.version@"
        description = "Queries Qdrant in order to gather a specified number of documents that are most closely related to the given query."
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

    QUERY = PropertyDescriptor(
        name="Query",
        description="The text of the query to send to Qdrant.",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )
    NUMBER_OF_RESULTS = PropertyDescriptor(
        name="Number of Results",
        description="The number of results to return from Qdrant.",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="10",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )
    FILTER = PropertyDescriptor(
        name="Metadata Filter",
        description='Optional metadata filter to apply with the query. For example: { "author": "john.doe" }',
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    properties = (
        QdrantUtils.QDRANT_PROPERTIES
        + QdrantUtils.EMBEDDING_MODEL_PROPERTIES
        + [
            QUERY,
            FILTER,
            NUMBER_OF_RESULTS,
            QueryUtils.OUTPUT_STRATEGY,
            QueryUtils.RESULTS_FIELD,
            QueryUtils.INCLUDE_METADATAS,
            QueryUtils.INCLUDE_DISTANCES,
        ]
    )

    embeddings = None
    query_utils = None
    client = None

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def onScheduled(self, context):
        self.client = QdrantClient(
            url=context.getProperty(QdrantUtils.QDRANT_URL).getValue(),
            api_key=context.getProperty(QdrantUtils.QDRANT_API_KEY).getValue(),
            prefer_grpc=context.getProperty(QdrantUtils.PREFER_GRPC).asBoolean(),
            https=context.getProperty(QdrantUtils.HTTPS).asBoolean(),
        )
        self.embeddings = create_embedding_service(context)
        self.query_utils = QueryUtils.QueryUtils(context)

    def transform(self, context, flowfile):
        collection_name = (
            context.getProperty(QdrantUtils.COLLECTION_NAME)
            .evaluateAttributeExpressions(flowfile)
            .getValue()
        )
        query = (
            context.getProperty(self.QUERY)
            .evaluateAttributeExpressions(flowfile)
            .getValue()
        )
        num_results = (
            context.getProperty(self.NUMBER_OF_RESULTS)
            .evaluateAttributeExpressions(flowfile)
            .asInteger()
        )
        filter = (
            context.getProperty(self.FILTER)
            .evaluateAttributeExpressions(flowfile)
            .getValue()
        )
        vector_store = Qdrant(
            client=self.client,
            collection_name=collection_name,
            embeddings=self.embeddings,
        )
        results = vector_store.similarity_search_with_score(
            query=query,
            k=num_results,
            filter=None if filter is None else json.loads(filter),
        )

        documents = []
        for result in results:
            documents.append(result[0].page_content)

        if context.getProperty(QueryUtils.INCLUDE_METADATAS).asBoolean():
            metadatas = []
            for result in results:
                metadatas.append(result[0].metadata)
        else:
            metadatas = None

        if context.getProperty(QueryUtils.INCLUDE_DISTANCES).asBoolean():
            distances = []
            for result in results:
                distances.append(result[1])
        else:
            distances = None

        (output_contents, mime_type) = self.query_utils.create_json(
            flowfile, documents, metadatas, None, distances, None
        )
        attributes = {"mime.type": mime_type}

        return FlowFileTransformResult(
            relationship="success", contents=output_contents, attributes=attributes
        )

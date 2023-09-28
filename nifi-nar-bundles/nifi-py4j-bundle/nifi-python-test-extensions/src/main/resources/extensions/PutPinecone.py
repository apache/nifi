import json

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

from langchain.vectorstores import  Pinecone
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.docstore.document import Document
from langchain.text_splitter import CharacterTextSplitter

import pinecone


class PutPinecone(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '2.0.0-SNAPSHOT'
        dependencies = ['openai==0.27.7', 'pinecone-client[grpc]==2.2.1', 'pinecone-datasets==0.5.0rc11', 'tqdm', 'langchain', 'tiktoken']
        description = 'Puts the contents of the FlowFile to the PineCone Vector DB'
        tags = ['pinecone', 'vector', 'db', 'ai']



    def __init__(self, **kwargs):
        # Build Property Descriptors
        self.pineconeApiKey = PropertyDescriptor(
            name="Pinecone API Key",
            description="The Pinecone API Key",
            required = True,
            sensitive = True,
            validators = [StandardValidators.NON_EMPTY_VALIDATOR]
        )
        self.env = PropertyDescriptor(
            name="Pinecone Environment",
            description="The Pinecone Environment to interact with",
            required = True,
            sensitive = False,
            default_value = "us-west4-gcp-free",
            validators = [StandardValidators.NON_EMPTY_VALIDATOR]
        )
        self.index = PropertyDescriptor(
            name="Pinecone Index",
            description="The name of the Pinecone Index to push data to",
            required = True,
            sensitive = False,
            expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
            validators = [StandardValidators.NON_EMPTY_VALIDATOR]
        )
        self.openAiApiKey = PropertyDescriptor(
            name="OpenAI API Key",
            description="API Key for OpenAI, which is used for creating embeddings",
            required=True,
            sensitive=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )
        self.descriptors = [self.index, self.env, self.pineconeApiKey, self.openAiApiKey]


    def onScheduled(self, context):
        # Initialize pinecone
        api_key = context.getProperty(self.pineconeApiKey.name).getValue()
        pinecone_env = context.getProperty(self.env.name).getValue()
        pinecone.init(api_key=api_key, environment=pinecone_env)

        openai_api_key = context.getProperty(self.openAiApiKey.name).getValue()
        self.embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)

        # Try whoami to verify that things are properly configured. This will throw an Exception when starting instead
        # of waiting for data to come in, if things are not properly configured.
        self.logger.info("Pinecone whoami response: {0}".format(pinecone.whoami()))


    def transform(self, context, flowFile):
        index_name = context.getProperty(self.index.name).evaluateAttributeExpressions(flowFile).getValue()

        flowfile_contents = str(flowFile.getContentsAsBytes())
        text_splitter = CharacterTextSplitter.from_tiktoken_encoder(chunk_size=5000, chunk_overlap=500)
        texts = text_splitter.split_text(flowfile_contents)

        docs = []
        for text in texts:
            metadata = {
                'flowfile_uuid': flowFile.getAttribute('uuid')
            }
            doc = Document(page_content=text, metadata=metadata)
            docs.append(doc)

        Pinecone.from_texts([t.page_content for t in docs], self.embeddings, index_name=index_name)

        return FlowFileTransformResult(relationship="success", contents=flowfile_contents)

    def getPropertyDescriptors(self):
        return self.descriptors

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

from langchain.vectorstores import Pinecone
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chains.question_answering import load_qa_chain
from langchain.chains import LLMChain
from langchain import PromptTemplate

from langchain.chat_models import ChatOpenAI

import pinecone


class QueryChatGPTWithVectorStore(FlowFileTransform):
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
        self.modelName = PropertyDescriptor(
            name="OpenAI Model Name",
            description="The name of the OpenAI model to use, such as gpt-4, gpt-3.5-turbo, etc.",
            required=True,
            sensitive=False,
            default_value="gpt-3.5-turbo",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )
        self.temperature = PropertyDescriptor(
            name="Temperature",
            description="The model temperature to use. A value of 0 makes the response more deterministic while a value of 1 is more creative.",
            required=True,
            sensitive=False,
            default_value="0",
            validators=[StandardValidators.NUMBER_VALIDATOR]
        )
        self.descriptors = [self.index, self.env, self.pineconeApiKey, self.openAiApiKey, self.modelName, self.temperature]


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

        # Load the question answering chain
        model_name = context.getProperty(self.modelName.name).getValue()
        temperature = context.getProperty(self.temperature.name).asFloat()
        llm = ChatOpenAI(temperature=temperature, openai_api_key=openai_api_key, model_name=model_name)
        #self.qa_chain = load_qa_chain(self.llm, chain_type="stuff")


        prompt_text = """
        Below, you will find both a question and some context that will be helpful in answering the question.
        Answer the question to the best of your ability, but do  not hallucinate.
        
        Context:
        ```
        {context}
        ```
        
        Question:
        ```
        {question}
        ```
        """
        prompt_template = PromptTemplate(
            template=prompt_text,
            input_variables=["context", "question"]
        )

        self.llm_chain = LLMChain(
            llm=llm,
            prompt=prompt_template,
            verbose=True
        )



    def transform(self, context, flowFile):
        index_name = context.getProperty(self.index.name).evaluateAttributeExpressions(flowFile).getValue()
        docsearch = Pinecone.from_existing_index(index_name=index_name, embedding=self.embeddings)
        question = str(flowFile.getContentsAsBytes())

        docs = docsearch.similarity_search(question)

        relevant_context = ""
        for doc in docs:
            relevant_context = relevant_context + str(doc.page_content) + "\n"

        relevant_context = relevant_context.replace("\\n", "\n")

        llm_args = {
            "context": relevant_context,
            "question": question
        }
        results = self.llm_chain(llm_args)
        answer = results['text']

        self.logger.info("Question: {0}".format(question))
        self.logger.info("Context: {0}".format(relevant_context))
        self.logger.info("Answer: {0}".format(answer))
        #answer = self.qa_chain.run(input_documents=docs, question=prompt)

        return FlowFileTransformResult(relationship = "success", contents=bytes(answer, 'utf-8'))


    def getPropertyDescriptors(self):
        return self.descriptors

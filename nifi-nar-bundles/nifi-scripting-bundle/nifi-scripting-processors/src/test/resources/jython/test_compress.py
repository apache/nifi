import traceback
# There is no Apache header on this file in order to reproduce the issue in NIFI-9596
#  where the first line gets appended to the last line of the additional modules
from callbacks import Compress, Decompress
from org.apache.nifi.processor import Processor
from org.apache.nifi.processor import Relationship
from org.apache.nifi.components import PropertyDescriptor

class CompressFlowFile(Processor) :
    __rel_success = Relationship.Builder().description("Success").name("success").build()

    def __init__(self) :
        pass

    def initialize(self, context) :
        pass

    def getRelationships(self) :
        return set([self.__rel_success])

    def validate(self, context) :
        pass

    def getPropertyDescriptors(self) :
        descriptor = PropertyDescriptor.Builder().name("mode").allowableValues("compress", "decompress").required(True).build()
        return [descriptor]

    def onPropertyModified(self, descriptor, newValue, oldValue) :
        pass

    def onTrigger(self, context, sessionFactory) :
        session = sessionFactory.createSession()
        try :
            # ensure there is work to do
            flowfile = session.get()
            if flowfile is None :
                return

            if context.getProperty("mode").getValue() == "compress" :
                flowfile = session.write(flowfile, Compress())
            else :
                flowfile = session.write(flowfile, Decompress())

            # transfer
            session.transfer(flowfile, self.__rel_success)
            session.commitAsync()
        except :
            print sys.exc_info()[0]
            print "Exception in TestReader:"
            print '-' * 60
            traceback.print_exc(file=sys.stdout)
            print '-' * 60

            session.rollback(true)
            raise

processor = CompressFlowFile()
class Relationship:
    def __init(self, name, description, auto_terminated=False):
        self.name = name
        self.description = description
        self.auto_terminated = auto_terminated

    def to_java_descriptor(self, gateway):
        return gateway.jvm.org.apache.nifi.processor.Relationship.Builder() \
            .name(self.name) \
            .description(self.description) \
            .autoTerminateDefault(self.auto_terminated) \
            .build()

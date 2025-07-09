from nifiapi.properties import PropertyDescriptor, PropertyDependency
from shared_props import SHARED_PROP

MY_PROP = PropertyDescriptor(
    name="my.property",
    description="This is my property",
    display_name="My Property",
    required=True,
    dependencies=[PropertyDependency(SHARED_PROP, ["value1", "value2"])]
)

class TestProcessorWithImport:
    def __init__(self):
        self._logger = None

    def set_logger(self, logger):
        self._logger = logger

    def get_property_descriptors(self):
        return [MY_PROP]

class PropertyDependencyTestCase(unittest.TestCase):
    def test_dependencies(self):
        processor = TestProcessorWithImport()
        descriptors = processor.get_property_descriptors()
        self.assertEqual(len(descriptors), 1)
        self.assertEqual(descriptors[0].dependencies[0].dependent_property.name, SHARED_PROP.name)

if __name__ == '__main__':
    unittest.main()

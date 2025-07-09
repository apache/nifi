import unittest
from nifiapi.properties import PropertyDescriptor, PropertyDependency
from shared_props import SHARED_PROP

class TestPropertyDependencyResolution(unittest.TestCase):

    def test_imported_dependency_is_resolved(self):
        # Simulate what would be created in a processor
        my_prop = PropertyDescriptor(
            name="my.property",
            description="A test property",
            display_name="My Property",
            required=True,
            dependencies=[PropertyDependency(SHARED_PROP, ["value1", "value2"])]
        )

        self.assertEqual(my_prop.dependencies[0].name, SHARED_PROP.name)
        self.assertEqual(my_prop.dependencies[0].dependent_values, ["value1", "value2"])

if __name__ == '__main__':
    unittest.main()

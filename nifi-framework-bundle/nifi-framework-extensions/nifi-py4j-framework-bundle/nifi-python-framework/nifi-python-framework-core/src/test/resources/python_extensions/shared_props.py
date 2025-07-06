from nifiapi.properties import PropertyDescriptor

SHARED_PROP = PropertyDescriptor(
    name="shared.property",
    description="Shared property defined in shared module",
    display_name="Shared Property",
    required=False
)
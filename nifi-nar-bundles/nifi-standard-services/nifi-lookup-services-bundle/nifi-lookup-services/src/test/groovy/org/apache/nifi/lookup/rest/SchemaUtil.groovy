package org.apache.nifi.lookup.rest

class SchemaUtil {
    static final String SIMPLE = """
        {
            "type": "record",
            "name": "SimpleRecord",
            "fields": [
                {
                    "name": "username",
                    "type": "string"
                },
                {
                    "name": "password",
                    "type": "string"
                }
            ]
        }
    """

    static final String COMPLEX = """{
        "type": "record",
        "name": "ComplexRecord",
        "fields": [
                {
                    "name": "top",
                    "type": {
                    "type": "record",
                    "name": "TopRecord",
                    "fields": [
                            {
                                "name": "middle",
                                "type": {
                                "name": "MiddleRecord",
                                "type": "record",
                                "fields": [
                                        {
                                            "name": "inner",
                                            "type": {
                                            "type": "record",
                                            "name": "InnerRecord",
                                            "fields": [
                                                    { "name": "username", "type": "string" },
                                                    { "name": "password", "type": "string" },
                                                    { "name": "email", "type": "string" }
                                            ]
                                        }
                                        }
                                ]
                            }
                            }
                    ]
                }
                }
        ]
    }"""
}

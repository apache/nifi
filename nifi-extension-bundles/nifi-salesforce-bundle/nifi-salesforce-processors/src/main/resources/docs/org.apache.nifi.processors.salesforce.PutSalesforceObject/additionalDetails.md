# PutSalesforceObject

### Description

Objects in Salesforce are database tables, their rows are known as records, and their columns are called fields. The
PutSalesforceObject creates a new a Salesforce record in a Salesforce object. The Salesforce object must be set as the 
"objectType" attribute of an incoming flowfile.
Check [Salesforce documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_list.htm)
for object types and metadata. The processor utilizes NiFi record-based processing to allow arbitrary input format.

#### Example

If the "objectType" is set to "Account", the following JSON input will create two records in the Account object with the
names "SampleAccount1" and "SampleAccount2".

```json
[
  {
    "name": "SampleAccount1",
    "phone": "1111111111",
    "website": "www.salesforce1.com",
    "numberOfEmployees": "100",
    "industry": "Banking"
  },
  {
    "name": "SampleAccount2",
    "phone": "22222222",
    "website": "www.salesforce2.com",
    "numberOfEmployees": "200",
    "industry": "Banking"
  }
]
```
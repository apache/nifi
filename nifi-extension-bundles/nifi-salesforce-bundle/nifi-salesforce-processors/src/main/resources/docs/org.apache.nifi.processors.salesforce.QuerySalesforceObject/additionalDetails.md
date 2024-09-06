<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# QuerySalesforceObject

### Description

Objects in Salesforce are database tables, their rows are known as records, and their columns are called fields. The
QuerySalesforceObject processor queries Salesforce objects and retrieves their records. The processor constructs the
query from processor properties or executes a custom SOQL (Salesforce Object Query Language) query and retrieves the
result record dataset using the Salesforce REST API. The 'Query Type' processor property allows the query to be built in
two ways. The 'Property Based Query' option allows to define a 'SELECT <fields> from <Salesforce object>' type query,
with the fields defined in the 'Field Names' property and the Salesforce object defined in the 'sObject Name' property,
whereas the 'Custom Query' option allows you to supply an arbitrary SOQL query. By using 'Custom Query', the processor
can accept an optional input flowfile and reference the flowfile attributes in the query. However, incremental loading
and record-based processing are only supported in 'Property Based Queries'.

### OAuth2 Access Token Provider Service

The OAuth2 Access Token Provider Service handles Salesforce REST API authorization. In order to use OAuth2
authorization, create a new StandardOauth2AccessTokenProvider service and configure it as follows.

* Authorization Server URL: It is the concatenation of the Salesforce URL and the token request service URL (
  /services/oauth2/token).
* Grant Type: User Password.
* Username: The email address registered in the Salesforce account.
* Password: For the Password a Security token must be requested. Go to Profile -> Settings and under the Reset My
  Security Token option, request one, which will be sent to the registered email address. The password is made up of the
  Salesforce account password and the Security token concatenated together without a space.
* Client ID: Create a new Connected App within Salesforce. Go to Setup -> On the left search panel find App Manager ->
  Create New Connected App. Once it’s done, the Consumer Key goes to the Client ID property.
* Client Secret: Available on the Connected App page under Consumer Secret.

### Age properties

The age properties are important to avoid processing duplicate records. Age filtering provides a sliding window that
starts with the processor’s prior run time and ends with the current run time minus the age delay. Only records that are
within the sliding window are queried and processed. On the processor, the Age Field property must be a datetime field
of the queried object, this will be subject to the condition that it is greater than the processor's previous but less
than the current run time (e.g. LastModifiedDate). The first run, for example, will query records whose LastModifiedDate
field is earlier than the current run time. The second will look for records with LastModifiedDate fields that are later
than the previous run time but earlier than the current run time.

The processor uses the Initial Age Filter as a specific timestamp that sets the beginning of the sliding window from
which processing builds the initial query. The format must adhere to the Salesforce SOQL standards (see Salesforce
documentation). The Age Delay moves the time of the records to be processed earlier than the current run time if
necessary.
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

# Apache NiFi CCDA Processor

CCDA Processor Bundle provides parser for Consolidated-CDA documents
This processor provides JSON in FlowFile content. Pretty Printing can be set as a property while configuring the processor.
Individual attributes are set as FlowFile attributes. The attributes are named as \<Parent\> \<dot\> \<Key\>.
If the Parent is repeating, the naming will be \<Parent\> \<underscore\> \<Parent Index\> \<dot\> \<Key\>. 

## Example JSON Output
	"observation": [
	  {
	    "id": [
	      {
	        "extension": "121387536",
	        "root": "2.16.840.1.113883.1.13.99999.999239"
	      }
	    ],
	    "values": {
	      "code": "194828000",
	      "codeSystem": "2.16.840.1.113883.6.96",
	      "codeSystemName": "SNOMED CT",
	      "displayName": "Angina (disorder)",
	      "translations": [
	        {
	          "code": "413.9",
	          "codeSystem": "2.16.840.1.113883.6.103",
	          "codeSystemName": "ICD-9CM (diagnosis codes)",
	          "displayName": "Other and unspecified angina pectoris"
	        }
	      ]
	    },
	    "statusCode": {
	      "code": "completed"
	    },
	    "effectiveTime": {
	      "low": "20130711"
	    }
	  }
	]


## Example Attribute Output
	problemSection.act_04.observation.effectiveTime.low=20130711
	problemSection.act_04.observation.id.extension=121387536
	problemSection.act_04.observation.id.root=2.16.840.1.113883.1.13.99999.999239
	problemSection.act_04.observation.statusCode.code=completed
	problemSection.act_04.observation.values.code=194828000
	problemSection.act_04.observation.values.codeSystem=2.16.840.1.113883.6.96
	problemSection.act_04.observation.values.codeSystemName=SNOMED CT
	problemSection.act_04.observation.values.displayName=Angina (disorder)
	problemSection.act_04.observation.values.translations.code=413.9
	problemSection.act_04.observation.values.translations.codeSystem=2.16.840.1.113883.6.103
	problemSection.act_04.observation.values.translations.codeSystemName=ICD-9CM (diagnosis codes)
	problemSection.act_04.observation.values.translations.displayName=Other and unspecified angina pectoris
	problemSection.act_04.statusCode.code=active

## Example Parser Mapping
This processor is driven by a mapping file which specifies the element relationships. For example

	org.openhealthtools.mdht.uml.cda.consol.impl.ProblemObservationImpl=id#element.ids\
		@values#element.values[0]\
		@statusCode#element.statusCode\
		@effectiveTime#element.effectiveTime\
		@negation#element.negationInd\
		@problemStatus#element.problemStatus
	org.openhealthtools.mdht.uml.cda.consol.impl.ProblemStatusImpl=id#element.id\
		@code#element.code\
		@values#element.values[0]\
		@statusCode#element.statusCode

## References
These mappings are defined as per the implementation guide - [HL7 Implementation Guide for CDA® Release 2](http://www.hl7.org/documentcenter/public/standards/dstu/CDAR2_IG_IHE_CONSOL_DSTU_R1dot1_2012JUL.zip)

CCDA sample file used in this bundle for testing is from [Cerner Sample - Transition of Care Referral Summary](https://github.com/chb/sample_ccdas/blob/master/Cerner%20Samples/Transition_of_Care_Referral_Summary.xml)

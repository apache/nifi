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

# UpdateAttribute

## Description:

This processor updates the attributes of a FlowFile using properties or rules that are added by the user. There are
three ways to use this processor to add or modify attributes. One way is the "Basic Usage"; this allows you to set
default attribute changes that affect every FlowFile going through the processor. The second way is the "Advanced
Usage"; this allows you to make conditional attribute changes that only affect a FlowFile if it meets certain
conditions. It is possible to use both methods in the same processor at the same time. The third way is the "Delete
Attributes Expression"; this allows you to provide a regular expression and any attributes with a matching name will be
deleted.

Please note that "Delete Attributes Expression" supersedes any updates that occur. If an existing attribute matches
the "Delete Attributes Expression", it will be removed whether it was updated or not. That said, the "Delete Attributes
Expression" only applies to attributes that exist in the input FlowFile, if it is added by this processor, the "Delete
Attributes Expression" will not detect it.

**Properties:**

The properties in this processor are added by the user. The expression language is supported in user-added properties
for this processor. See the NiFi Expression Language Guide to learn how to formulate proper expression language
statements to perform the desired functions.

If an Attribute is added with the name **alternate.identifier** and that attribute's value is a URI, an ADD\_INFO
Provenance Event will be registered, correlating the FlowFile with the given alternate identifier.

**Relationships:**

* success
    * If the processor successfully updates the specified attribute(s), then the FlowFile follows this relationship.
* set state fail
    * If the processor is running statefully, and fails to set the state after adding attributes to the FlowFile, then
      the FlowFile will be routed to this relationship.

**Basic Usage**

For basic usage, changes are made by adding a new processor property and referencing as its name the attribute you want
to change. Then enter the desired attribute value as the Value. The Value can be as simple as any text string or it can
be a NiFi Expression Language statement that specifies how to formulate the value. (See the NiFi Expression Language
Usage Guide for details on crafting NiFi Expression Language statements.)

As an example, to alter the standard "filename" attribute so that it has ".txt" appended to the end of it, add a new
property and make the property name "filename" (to reference the desired attribute), and as the value, use the NiFi
Expression Language statement shown below:

* **Property**: filename
* **Value**: `${filename}.txt`

The preceding example illustrates how to modify an existing attribute. If an attribute does not already exist, this
processor can also be used to add a new attribute. For example, the following property could be added to create a new
attribute called myAttribute that has the value myValue:

* **Property**: myAttribute
* **Value**: myValue

In this example, all FlowFiles passing through this processor will receive an additional FlowFile attribute called
myAttribute with the value myValue. This type of configuration might be used in a flow where you want to tag every
FlowFile with an attribute so that it can be used later in the flow, such as for routing in a RouteOnAttribute
processor.

**Advanced Usage**

The preceding examples illustrate how to make changes to every FlowFile that goes through the processor. However, the
UpdateAttribute processor may also be used to make conditional changes.

To change attributes based on some condition, use the Advanced User Interface (UI) in the processor by clicking the *
*Advanced** menu item in the Canvas context menu.

Clicking the Advanced menu item displays the Advanced UI. In the Advanced UI, Conditions and their associated Actions are
entered as "Rules". Each rule basically says, "If these conditions are met, then do this action." One or more conditions
may be used in a given rule, and they all must be met in order for the designated action(s) to be taken.

**Adding Rules**

To add the first rule, click on the "Create Rule" button in center of the screen. The Edit Rule form will display where
the name, comments, conditions, and actions for the rule can be entered. Once the rule is defined, click the **Add** 
button. Additional rules can be added by clicking the button with the plus symbol located to the top right of the 
Rule listing.

**Example Rules**

This example has two rules: CheckForLargeFiles and CheckForGiantFiles. The CheckForLargeFiles rule has these conditions:

* `${filename:equals('fileOfInterest')}`
* `${fileSize:toNumber():ge(1048576)}`
* `${fileSize:toNumber():lt(1073741824)}`

Then it has this action for the filename attribute:

* `${filename}.meg`

Taken together, this rule says:

* If the value of the filename attribute is fileOfInterest, **and**
* If the fileSize is greater than or equal to (ge) one megabyte (1,048,576 bytes), **and**
* If the fileSize is less than (lt) one gigabyte (1,073,741,824 bytes)
* Then change the value of the filename attribute by appending ".meg" to the filename.

**Adding another Rule**

Continuing with this example, we can add another rule to check for files that are larger than one gigabyte. When we add
this second rule, we can use the previous rule as a template, so to speak, by taking advantage of the "Clone Rule" option 
in the menu for the Rule that you wan to clone. This will open with new Rule form with the exisitng Rules criteria
pre-populated.

In this example, the CheckForGiantFiles rule has these conditions:

* `${filename:equals('fileOfInterest')}`
* `${fileSize:toNumber():gt(1073741824)}`

Then it has this action for the filename attribute:

* ` ${filename}.gig`

Taken together, this rule says:

* If the value of the filename attribute is fileOfInterest, **and**
* If the fileSize is greater than (gt) one gigabyte (1,073,741,824 bytes)
* Then change the value of the filename attribute by appending ".gig" to the filename.

**Combining the Basic Usage with the Advanced Usage**

The UpdateAttribute processor allows you to make both basic usage changes (i.e., to every FlowFile) and advanced usage
changes (i.e., conditional) at the same time; however, if they both affect the same attribute(s), then the conditional
changes take precedence. This has the added benefit of supporting a type of "else" construct. In other words, if none of
the rules match for the attribute, then the basic usage changes will be made.

**Deleting Attributes**

Deleting attributes is a simple as providing a regular expression for attribute names to be deleted. This can be a
simple regular expression that will match a single attribute or more complex regular expression to match a group of
similarly named attributes or even several individual attribute names.

* **lastUser** - will delete an attribute with the name "lastUser".
* **user.&ast;** - will delete attributes beginning with "user", including for example "username, "userName", "userID",
  and "users". But it will not delete "User" or "localuser".
* **(user.\*|host.\*|.\*Date)** - will delete "user", "username", "userName", "hostInfo", "hosts", and "updateDate", but
  not "User", "HOST", "update", or "updatedate".

The delete attributes function does not produce a Provenance Event if the **alternate.identified** Attribute is deleted.

**FlowFile Policy**

Another setting in the Advanced UI is the FlowFile Policy. It is located in the upper-left corner of the UI, and it
defines the processor's behavior when multiple rules match. It may be changed using the slide toggle. By default, the
FlowFile Policy is set to use a clone of the original FlowFile for each matching rule.

If the FlowFile policy is set to "use clone", and multiple rules match, then a copy of the incoming FlowFile is created,
such that the number of outgoing FlowFiles is equal to the number of rules that match. In other words, if two rules (A
and B) both match, then there will be two outgoing FlowFiles, one for Rule A and one for Rule B. This can be useful in
situations where you want to add an attribute to use as a flag for routing later. In this example, there will be two
copies of the file available, one to route for the A path, and one to route for the B path.

If the FlowFile policy is set to "use original", then all matching rules are applied to the same incoming FlowFile, and
there is only one outgoing FlowFile with all the attribute changes applied. In this case, the order of the rules matters
and the action for each rule that matches will be applied in that order. If multiple rules contain actions that update
the same attribute, the action from the last matching rule will take precedence. Notably, you can drag and drop the
rules into a certain order within the Rules list once the FlowFile Policy is set to "use original" and the user has
toggled the "Reorder rules" control. While in this reordering mode, other Rule modifications are not allowed.

**Filtering Rules**

The Advanced UI supports the creation of an arbitrarily large number of rules. In order to manage large rule sets, the
listing of rules may be filtered using the Filter mechanism in the lower left corner. Rules may be filtered by any text
in the name, condition, or action.

**Closing the Advanced UI**

Once all changes have been saved in the Advanced UI, you can navigate back to the Canvas using the navigation at the top.

**Stateful Usage**

By selecting "store state locally" option for the "Store State" property UpdateAttribute will not only store the
evaluated properties as attributes of the FlowFile but also as stateful variables to be referenced in a recursive
fashion. This enables the processor to calculate things like the sum or count of incoming FlowFiles. A dynamic property
can be referenced as a stateful variable like so:

* Dynamic Property
    * key : theCount
    * value : `${getStateValue("theCount"):plus(1)}`

This example will keep a count of the total number of FlowFiles that have passed through the processor. To use logic on
top of State, simply use the "Advanced Usage" of UpdateAttribute. All Actions will be stored as stateful attributes as
well as being added to FlowFiles. Using the "Advanced Usage" it is possible to keep track of things like a maximum value
of the flow so far. This would be done by having a condition of "${getStateValue("maxValue"):lt(${value})}" and an
action of attribute:"maxValue", value:"${value}". The "Stateful Variables Initial Value" property is used to initialize
the stateful variables and is required to be set if running statefully. Some logic rules will require a very high
initial value, like using the Advanced rules to determine the minimum value. If stateful properties reference other
stateful properties then the value for the other stateful properties will be an iteration behind. For example,
attempting to calculate the average of the incoming stream requires the sum and count. If all three properties are set
in the same UpdateAttribute (like below) then the Average will always not include the most recent values of count and
sum:

* Count
    * key : theCount
    * value : `${getStateValue("theCount"):plus(1)}`
* Sum
    * key : theSum
    * value : `${getStateValue("theSum"):plus(${flowfileValue})}`
* Average
    * key : theAverage
    * value : `${getStateValue("theSum"):divide(getStateValue("theCount"))}`

Instead, since average only relies on theCount and theSum attributes (which are added to the FlowFile as well) there
should be a following Stateless UpdateAttribute which properly calculates the average. In the event that the processor
is unable to get the state at the beginning of the onTrigger, the FlowFile will be pushed back to the originating
relationship and the processor will yield. If the processor is able to get the state at the beginning of the onTrigger
but unable to set the state after adding attributes to the FlowFile, the FlowFile will be transferred to "set state
fail". This is normally due to the state not being the most recent version (another thread has replaced the state with
another version). In most use-cases this relationship should loop back to the processor since the only affected
attributes will be overwritten. Note: Currently the only "stateful" option is to store state locally. This is done
because the current implementation of clustered state relies on Zookeeper and Zookeeper isn't designed for the type of
load/throughput UpdateAttribute with state would demand. In the future, if/when multiple different clustered state
options are added, UpdateAttribute will be updated.

**Combining the Advanced Usage with Stateful**

The UpdateAttribute processor allows you to use both advanced usage changes (i.e., conditional) in addition to storing
the values in state at the same time. This allows UpdateAttribute to act as a stateful rules engine to enable powerful
concepts such as a Finite-State machine or keeping track of a min/max value. Working with both is relatively simple,
when the processor would normally update an attribute on the processor (ie. it matches a conditional rule) the same
update is stored to state. Referencing state via the advanced tab is done in the same way too, using "getStateValue".
Note: In the event the "use clone" policy is set and the state is failed to set, no clones will be generated and only
the original FlowFile will be transferred to "set state fail".

**Notes about Concurrency and Stateful Usage**

When using the stateful option, concurrent tasks should be used with caution. If every incoming FlowFile will update
state then it will be much more efficient to have only one task. This is because the first thing the onTrigger does is
get the state and the last thing it does is store the state if there are an updates. If it does not have the most recent
initial state when it goes to update it will fail and send the FlowFile to "set state fail". This is done so that the
update is successful when it was done with the most recent information. If it didn't do it in this mock-atomic way,
there'd be no guarantee that the state is accurate. When considering Concurrency, the use-cases generally fall into one
of three categories:

* A data stream where each FlowFile updates state ex. updating a counter
* A data stream where a FlowFile doesn't always update state ex. a Finite-State machine
* A data stream that doesn't update state, and a second "control" stream that one updates every time but is rare
  compared to the data stream ex. a trigger

The first and last cases are relatively clear-cut in their guidance. For the first, concurrency should not be used.
Doing so will just waste CPU and any benefits of concurrency will be wiped due to misses in state. For the last case, it
can easily be done using concurrency. Since updates are rare in the first place it will be even more rare that two
updates are processed at the same time that cause problems. The second case is a bit of a grey area. If updates are rare
then concurrency can probably be used. If updates are frequent then concurrency would probably cause more problems than
benefits. Regardless, testing to determine the appropriate tuning is the only true answer.
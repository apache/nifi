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

# Wait

## Best practices to handle multiple signal ids at a Wait processor

When a Wait processor is expected to process multiple signal ids, by configuring 'Release Signal Identifier' with a
FlowFile attribute Expression Language, there are few things to consider in order to get the expected result. Processor
configuration can vary based on your requirement. Also, you will need to have high level understanding on how Wait
processor works:

* The Wait processor only processes a single signal id at a time
* How frequently the Wait processor runs is defined in the 'Run Schedule'
* Which FlowFile is processed is determined by a Prioritizer
* Not limited to the Wait processor, but for all processors, the order of queued FlowFiles in a connection is undefined
  if no Prioritizer is set

See following sections for common patterns

* [Release any FlowFile as soon as its signal is notified](#asap)
* [Release higher priority FlowFiles in each signal id](#higher-priority)

### Release any FlowFile as soon as its signal is notified

This is the most common use case. FlowFiles are independent and can be released in any order.

#### Important configurations:

* Use FirstInFirstOutPrioritizer (FIFO) at 'wait' relationship (or the incoming connection if 'Wait Mode' is 'Keep in
  the upstream connection)

The following table illustrates the notified signal ids, queued FlowFiles and what will happen at each Wait run cycle.

| \# of Wait run | Notified Signals | Queue Index (FIFO) | FlowFile UUID | Signal ID |                                                                                                         |
|----------------|------------------|--------------------|---------------|-----------|---------------------------------------------------------------------------------------------------------|
| 1              | B                | 1                  | a             | A         | This FlowFile is processed. But its signal is not found, and will be re-queued at the end of the queue. |
| 2              | b                | B                  |               |
| 3              | c                | C                  |               |
| 2              | B                | 1                  | b             | B         | This FlowFile is processed and since its signal is notified, this one will be released to 'success'.    |
| 2              | c                | C                  |               |
| 3              | a                | A                  |               |
| 3              |                  | 1                  | c             | C         | This FlowFile will be processed at the next run.                                                        |
| 2              | a                | A                  |               |

### Release higher priority FlowFiles in each signal id

Multiple FlowFiles share the same signal id, and the order of releasing a FlowFile is important.

#### Important configurations:

* Use a (or set of a) Prioritizer(s) suites your need other than FIFO, at 'wait' relationship (or the incoming
  connection if 'Wait Mode' is 'Keep in the upstream connection), e.g. PriorityPrioritizer
* Specify adequate 'Wait Penalty Duration', e.g. "3 sec",
* 'Wait Penalty Duration' should be grater than 'Run Schedule', e.g "3 sec" > "1 sec"
* Increase 'Run Duration' to avoid the limitation of number of signal ids (see the [note](#run-duration) below)

The following table illustrates the notified signal ids, queued FlowFiles and what will happen at each Wait run cycle.
The example uses PriorityPrioritizer to control the order of processing FlowFiles within a signal id. If 'Wait Penalty
Duration' is configured, Wait processor tracks unreleased signal ids and their penalty representing when they will be
checked again.

| \# of Wait run | Notified Signals | Signal Penalties | Queue Index (via 'priority' attribute) | FlowFile UUID                                                                                                                                    | Signal ID | 'priority' attr |                                                                                                                                                         |
|----------------|------------------|------------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 (00:01)      | B                |                  | 1                                      | a-1                                                                                                                                              | A         | 1               | This FlowFile is processed. But its signal is not found. Penalized.                                                                                     |
| 2              | b-1              | B                | 1                                      | Since a-1 and b-1 have the same priority '1', b-1 may be processed before a-1. You can add another Prioritizer to define more specific ordering. |
| 3              | b-2              | B                | 2                                      |                                                                                                                                                  |
| 2 (00:02)      | B                | A (00:04)        | 1                                      | a-1                                                                                                                                              | A         | 1               | This FlowFile is the first one according to the configured Prioritizer, but the signal id is penalized. So, this FlowFile is skipped at this execution. |
| 2              | b-1              | B                | 1                                      | This FlowFile is processed.                                                                                                                      |
| 3              | b-2              | B                | 2                                      |                                                                                                                                                  |
| 3 (00:03)      |                  | A (00:04)        | 1                                      | a-1                                                                                                                                              | A         | 1               | This FlowFile is the first one but is still penalized.                                                                                                  |
| 2              | b-2              | B                | 2                                      | This FlowFile is processed, but its signal is not notified yet, thus will be penalized.                                                          |
| 4 (00:04)      |                  | B (00:06)        | 1                                      | a-1                                                                                                                                              | A         | 1               | This FlowFile is no longer penalized, and get processed. But its signal is not notified yet, thus will be penalized again.                              |
| 2              | b-2              | B                | 2                                      |                                                                                                                                                  |

#### The importance of 'Run Duration' when 'Wait Penalty Duration' is used

There are limitation of number of signals can be checked based on the combination of 'Run Schedule' and 'Wait Penalize
Duration'. If this limitation is engaged, some FlowFiles may not be processed and remain in the 'wait' relationship even
if their signal ids are notified. Let's say Wait is configured with:

* Run Schedule = 1 sec
* Wait Penalize Duration = 3 sec
* Release Signal Identifier = ${uuid}

And there are 5 FlowFiles F1, F2 ... F5 in the 'wait' relationship. Then the signal for F5 is notified. Wait will work
as follows:

* At 00:00 Wait checks the signal for F1, not found, and penalize F1 (till 00:03)
* At 00:01 Wait checks the signal for F2, not found, and penalize F2 (till 00:04)
* At 00:02 Wait checks the signal for F3, not found, and penalize F3 (till 00:05)
* At 00:03 Wait checks the signal for F4, not found, and penalize F4 (till 00:06)
* At 00:04 Wait checks the signal for F1 again, because it's not penalized any longer

Repeat above cycle, thus F5 will not be released until one of F1 ... F4 is released.

To mitigate such limitation, increasing 'Run Duration' is recommended. By increasing 'Run Duration', Wait processor can
keep being scheduled for that duration. For example, with 'Run Duration' 500 ms, Wait should be able to loop through all
5 queued FlowFiles at a single run.

## Using counters

A counter is basically a label to differentiate signals within the cache. (A cache in this context is a "container" that
contains signals that have the same signal identifier.)

Let's suppose that there are the following signals in the cache (note, that these are not FlowFiles on the incoming (or
wait) connection of the Wait processor, like in the examples above, but release signals stored in the cache.)

| Signal ID | Signal Counter Name |
|-----------|---------------------|
| A         | counter\_1          |
| A         | counter\_1          |
| A         | counter\_2          |

In this state, the following FlowFile gets processed by the Wait processor, (the FlowFile has a signal\_counter\_name
attribute and the Wait processor is configured to use the value of this attribute as the value of the Signal Counter
Name property):

| FlowFile UUID | Signal ID | signal\_counter\_name |
|---------------|-----------|-----------------------|
| a-1           | A         | counter\_3            |

Despite the fact that the cache identified by Signal ID "A" has signals in it, the FlowFile above will be sent to the '
wait' relationship, since there is no signal in the cache that belongs to the counter named "counter\_3".

Let's suppose, that the state of the cache is the same as above, and the following FlowFile gets processed by the Wait
processor:

| FlowFile UUID | Signal ID | signal\_counter\_name |
|---------------|-----------|-----------------------|
| a-2           | A         | counter\_1            |

The FlowFile is transmitted to the 'success' relationship, since cache "A" has signals in it and there are signals that
belong to "counter\_1". The outgoing FlowFile will have the following attributes and their values appended to it:

* wait.counter.counter\_1 : 2
* wait.counter.counter\_2 : 1
* wait.counter.total : 3

The key point here is that counters can be used to differentiate between signals within the cache. If counters are used,
a new attribute will be appended to the FlowFile passing the Wait processor for each counter. If a large number of
counters are used within a cache, the FlowFile passing the Wait processor will have a large number of attributes
appended to it. To avoid that, it is recommended to use multiple caches with a few counters in each, instead of one
cache with many counters.

For example:

* Cache identified by Release Signal ID "A" has counters: "counter\_1" and "counter\_2"
* Cache identified by Release Signal ID "B" has counters: "counter\_3" and "counter\_4"
* Cache identified by Release Signal ID "C" has counters: "counter\_5" and "counter\_6"

(Counter names do not need to be unique between caches, the counter name(s) used in cache "A" could be reused in cache "
B" and "C" as well.)
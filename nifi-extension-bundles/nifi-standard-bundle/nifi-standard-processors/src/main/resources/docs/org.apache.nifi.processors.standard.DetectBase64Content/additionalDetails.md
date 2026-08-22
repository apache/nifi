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

# DetectBase64Content

This processor reports whether FlowFile content looks like Base64 without decoding it. Attempting a decode is an
unreliable way to answer that question: some content that is not Base64 decodes without error, and some content
that is Base64 fails to decode for unrelated reasons. Inspecting the characters avoids both problems, and the
FlowFile is always routed to `success` so that the answer arrives as an attribute rather than as a failed route.

## Detection Rules

Content is reported as `true` when all of the following hold:

* It contains at least one Base64 character.
* Every character is either from the Base64 alphabet, a padding character, or a line separator.
* No Base64 characters appear after a padding character.
* There are no more than two padding characters.
* The number of Base64 and padding characters is a multiple of four. This is only checked when the entire content
  is read — see [Detection Scope](#detection-scope) below.

Anything else is reported as `false`.

The alphabet is the standard one from [RFC 4648 Section 4](https://datatracker.ietf.org/doc/html/rfc4648#section-4):
`A-Z`, `a-z`, `0-9`, `+` and `/`, with `=` as the padding character. Carriage return and line feed are accepted
anywhere so that content wrapped into fixed-width lines, as MIME Base64 is, still matches. Other whitespace is not
accepted: a space would make ordinary prose such as `the quick brown fox` look like Base64.

The URL-safe alphabet, which uses `-` and `_` in place of `+` and `/`, is not recognised and is reported as `false`.

## Detection Scope

`Entire Content` reads the whole content and gives a definitive answer. The content is streamed, not buffered, so
memory use stays flat no matter how large the FlowFile is. Reading also stops at the first character that rules
Base64 out, so content that is obviously not Base64 costs very little even when it is large.

`Sample` reads at most `Sample Size` bytes from the beginning. This bounds the read on large content, at the cost
of certainty: content that starts with valid Base64 characters but turns invalid later is reported as `true`. When
the content turns out to be smaller than the sample size, the entire content has been read and the result is
definitive.

## A Note on Short Content

Detection works on the characters alone, so any run of alphabet characters whose length is a multiple of four is
valid Base64. The word `test` decodes to three bytes and is reported as `true`. No content-only check can
distinguish that from intentionally encoded data, so treat a `true` result on very short content accordingly.

## Branching on the Result

The result is written to the attribute named by `Base64 Attribute Name`, which defaults to `content.base64`. To act
on it, connect `success` to a RouteOnAttribute processor and add a property whose value is an expression such as:

```
${content.base64:equals('true')}
```

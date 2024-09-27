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

# ConsumeSlack

## Description:

ConsumeSlack allows for receiving messages from Slack using Slack's `conversations.history` API. This allows for
consuming message events for a given conversation, such as a Channel. The Processor periodically polls Slack in order to
obtain the latest messages. Unfortunately, the Slack API does not provide a mechanism for easily identifying new replies
to messages (i.e., new threaded messages), without scanning through the original "parent" messages as well. As a result,
the Processor will periodically poll messages within a channel in order to find any new replies. By default, this occurs
every 5 minutes, but this can be configured by changing the value of the "Reply Monitor Frequency" property.
Additionally, for long-lived channels, polling all messages would be very expensive. As a result, the Processor only
polls messages newer than 7 days (by default) for new replies. This can be configured by setting the value of the "Reply
Monitor Window" property.

## Slack Setup

In order use this Processor, it requires that a Slack App be created and installed in your Slack workspace. An OAuth
User or Bot Token must be created for the App, and the token must have the `channels:history`, `groups:history`,
`im:history`, or `mpim:history` User Token Scope. Which scope is necessary depends on the type of conversation to
consume from. Please see [Slack's documentation](https://api.slack.com/start/quickstart) for the latest information on
how to create an Application and install it into your workspace.

Depending on the Processor's configuration, you may also require additional Scopes. For example, the Channels to consume
from may be listed either as a Channel ID or (for public Channels) a Channel Name. However, if a name, such as
`#general` is used, the token must be provided the `channels:read` scope in order to determine the Channel ID for you.
Additionally, if the "Resolve Usernames" property is set to true, the token must have the `users:read` scope in order to
resolve the User ID to a Username.

Rather than requiring the `channels:read` Scope, you may alternatively supply only Channel IDs for the "Channel"
property. To determine the ID of a Channel, navigate to the desired Channel in Slack. Click the name of the Channel at
the top of the screen. This provides a popup that provides information about the Channel. Scroll to the bottom of the
popup, and you will be shown the Channel ID with the ability to click a button to Copy the ID to your clipboard.

At the time of this writing, the following steps may be used to create a Slack App with the necessary scope of
`channels:history` scope. However, these instructions are subject to change at any time, so it is best to read
through [Slack's Quickstart Guide](https://api.slack.com/start/quickstart).

* Create a Slack App. Click [here](https://api.slack.com/apps) to get started. From here, click the "Create New App"
  button and choose "From scratch." Give your App a name and choose the workspace that you want to use for developing
  the app.
* Creating your app will take you to the configuration page for your application. For example,
  `https://api.slack.com/apps/<APP_IDENTIFIER>`. From here, click on "OAuth & Permissions" in the left-hand menu. Scroll
  down to the "Scopes" section and click the "Add an OAuth Scope" button under 'Bot Token Scopes'. Choose the
  `channels:history` scope.
* Scroll back to the top, and under the "OAuth Tokens for Your Workspace" section, click the "Install to Workspace"
  button. This will prompt you to allow the application to be added to your workspace, if you have the appropriate
  permissions. Otherwise, it will generate a notification for a Workspace Owner to approve the installation.
  Additionally, it will generate a "Bot User OAuth Token".
* Copy the value of the "Bot User OAuth Token." This will be used as the value for the ConsumeSlack Processor's
  `Access Token` property.
* The Bot must then be enabled for each Channel that you would like to consume messages from. In order to do that, in
  the Slack application, go to the Channel that you would like to consume from and press `/`. Choose the
  `Add apps to this channel` option, and add the Application that you created as a Bot to the channel.
* Alternatively, instead of creating an OAuth Scope of `channels:history` under "Bot Token Scopes", you may choose to
  create an OAuth Scope of `channels:history` under the "User Token Scopes" section. This will allow the token to be
  used on your behalf in any channel that you have access to, such as all public channels, without the need to
  explicitly add a Bot to the channel.
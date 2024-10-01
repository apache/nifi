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

# PublishSlack

## Description:

PublishSlack allows for the ability to send messages to Slack using Slack's `chat.postMessage` API. This Processor
should be preferred over the deprecated PutSlack and PostSlack Processors, as it aims to incorporate the capabilities of
both of those Processors, improve the maintainability, and ease the configuration for the user.

## Slack Setup

In order use this Processor, it requires that a Slack App be created and installed in your Slack workspace. An OAuth
User or Bot Token must be created for the App. The token must have the `chat:write` Token Scope. Please
see [Slack's documentation](https://api.slack.com/start/quickstart) for the latest information on how to create an
Application and install it into your workspace.

Depending on the Processor's configuration, you may also require additional Scopes. For example, if configured to upload
the contents of the FlowFile as a message attachment, the `files:write` User Token Scope or Bot Token Scope must be
granted. Additionally, the Channels to consume from may be listed either as a Channel ID or (for public Channels) a
Channel Name. However, if a name, such as `#general` is used, the token must be provided the `channels:read` scope in
order to determine the Channel ID for you.

Rather than requiring the `channels:read` Scope, you may alternatively supply only Channel IDs for the "Channel"
property. To determine the ID of a Channel, navigate to the desired Channel in Slack. Click the name of the Channel at
the top of the screen. This provides a popup that provides information about the Channel. Scroll to the bottom of the
popup, and you will be shown the Channel ID with the ability to click a button to Copy the ID to your clipboard.

At the time of this writing, the following steps may be used to create a Slack App with the necessary scope of
`chat:write` scope. However, these instructions are subject to change at any time, so it is best to read
through [Slack's Quickstart Guide](https://api.slack.com/start/quickstart).

* Create a Slack App. Click [here](https://api.slack.com/apps) to get started. From here, click the "Create New App"
  button and choose "From scratch." Give your App a name and choose the workspace that you want to use for developing
  the app.
* Creating your app will take you to the configuration page for your application. For example,
  `https://api.slack.com/apps/<APP_IDENTIFIER>`. From here, click on "OAuth & Permissions" in the left-hand menu. Scroll
  down to the "Scopes" section and click the "Add an OAuth Scope" button under 'Bot Token Scopes'. Choose the
  `chat:write` scope.
* Scroll back to the top, and under the "OAuth Tokens for Your Workspace" section, click the "Install to Workspace"
  button. This will prompt you to allow the application to be added to your workspace, if you have the appropriate
  permissions. Otherwise, it will generate a notification for a Workspace Owner to approve the installation.
  Additionally, it will generate a "Bot User OAuth Token".
* Copy the value of the "Bot User OAuth Token." This will be used as the value for the ConsumeSlack Processor's
  `Access Token` property.
* The Bot must then be enabled for each Channel that you would like to consume messages from. In order to do that, in
  the Slack application, go to the Channel that you would like to consume from and press `/`. Choose the
  `Add apps to this channel` option, and add the Application that you created as a Bot to the channel.
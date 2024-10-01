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

# ListenSlack

## Description:

ListenSlack allows for receiving real-time messages and commands from Slack using Slack's Events API. This Processor
does not provide any capabilities for retrieving historical messages. However, the ConsumeSlack Processor provides the
ability to do so. This Processor is generally used when implementing a bot in NiFi, or when it is okay to lose messages
in the case that NiFi or this Processor is stopped for more than 5 minutes.

This Processor may be used to listen for Message Events, App Mention Events (when the bot user is mentioned in a
message) or Slack Commands. For example, you may wish to create a Slack App that receives the `/nifi` command and when
received, performs some task. The Processor does not allow listening for both Message Events and Commands, as the output
format is very different for the two, and this would lead to significant confusion. Instead, if there is a desire to
consume both Message Events and Commands, two ListenSlack Processors should be used - one for Messages and another the
Commands.

Note that unlike the ConsumeSlack Processor, ListenSlack does not require that a Channel name or ID be provided. This is
because the Processor listens for Events/Commands from all channels (and "channel-like" conversations) that the
application has been added to.

## Slack Setup

In order use this Processor, it requires that a Slack App be created and installed in your Slack workspace.
Additionally, the App must have Socket Mode enabled. Please
see [Slack's documentation](https://api.slack.com/start/quickstart) for the latest information on how to create an
Application and install it into your workspace.

At the time of this writing, the following steps may be used to create a Slack App with the necessary scopes. However,
these instructions are subject to change at any time, so it is best to read
through [Slack's Quickstart Guide](https://api.slack.com/start/quickstart).

* Create a Slack App. Click [here](https://api.slack.com/apps) to get started. From here, click the "Create New App"
  button and choose "From scratch." Give your App a name and choose the workspace that you want to use for developing
  the app.
* Creating your app will take you to the configuration page for your application. For example,
  `https://api.slack.com/apps/<APP_IDENTIFIER>`. From here, click on "Socket Mode" and flip the toggle for "Enable
  Socket Mode." Accept the default scope and apply the changes. From here, click on "Event Subscriptions."
* Flip the toggle to turn on "Enable Events." In the "Subscribe to bot events" section, add the following Bot User
  Events: `app_mention`, `message.channels`, `message.groups`, `message.im`, `message.mpim`. Click "Save Changes" at the
  bottom of the screen.
* Click on the "OAuth & Permissions" link on the left-hand side. Under the "OAuth Tokens for Your Workspace" section,
  click the "Install to Workspace" button. This will prompt you to allow the application to be added to your workspace,
  if you have the appropriate permissions. Otherwise, it will generate a notification for a Workspace Owner to approve
  the installation. Additionally, it will generate a "Bot User OAuth Token".
* The Bot must then be enabled for each Channel that you would like to consume messages from. In order to do that, in
  the Slack application, go to the Channel that you would like to consume from and press `/`. Choose the
  `Add apps to this channel` option, and add the Application that you created as a Bot to the channel.
* Additionally, if you would like your Bot to receive commands, navigate to the "Slash Commands" section on the
  left-hand side. Create a New Command and complete the form. If you have already installed the app in a workspace, you
  will need to re-install your app at this time, in order for the changes to take effect. You should be prompted to do
  so with a link at the top of the page. Now, whenever a user is in a channel with your App installed, the user may send
  a command. For example, if you configured your command to be `/nifi` then a user can trigger your bot to receive the
  command by simply typing `/nifi` followed by some text. If your Processor is running, it will receive the command and
  output it. Otherwise, the user will receive an error.

## Configuring the Tokens

Now that your Slack Application has been created and configured, you will need to provide the ListenSlack Processor with
two tokens: the App Token and the Bot token. To get the App Token, go to your Slack Application's configuration page. On
the left-hand side, navigate to "Basic Information." Scroll down to "App-Level Tokens" and click on the token that you
created in the Slack Setup section above. This will provide you with a pop-up showing your App Token. Click the "Copy"
button and paste the value into your Processor's configuration. Then click "Done" to close the popup.

To obtain your Bot Token, again in the Slack Application's configuration page, navigate to the "OAuth & Permissions"
section on the left-hand side. Under the "OAuth Tokens for Your Workspace" section, click the "Copy" button under the "
Bot User OAuth Token" and paste this into your NiFi Processor's configuration.
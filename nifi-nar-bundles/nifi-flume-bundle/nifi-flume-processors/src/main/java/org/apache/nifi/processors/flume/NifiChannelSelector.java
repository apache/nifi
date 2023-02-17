/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.flume;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;


public class NifiChannelSelector implements ChannelSelector {
  private String name;
  private final List<Channel> requiredChannels;
  private final List<Channel> optionalChannels;

  public NifiChannelSelector(Channel channel) {
    requiredChannels = ImmutableList.of(channel);
    optionalChannels = ImmutableList.of();
  }

  @Override
  public List<Channel> getRequiredChannels(Event event) {
    return requiredChannels;
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    return optionalChannels;
  }

  @Override
  public List<Channel> getAllChannels() {
    return requiredChannels;
  }

  @Override
  public void setChannels(List<Channel> channels) {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void configure(Context context) {
  }
}

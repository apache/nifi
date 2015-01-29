
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

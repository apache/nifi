package org.apache.nifi.web.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;

public class Sorters {

	public static ArrayList<Port> sortPorts(ArrayList<Port> list) {
		Collections.sort(list, new Comparator<Port>() {

			@Override
			public int compare(Port o1, Port o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});

		return list;
	}

	public static ArrayList<RemoteProcessGroup> sortRemoteProcessGroups(
			ArrayList<RemoteProcessGroup> list) {
		Collections.sort(list, new Comparator<RemoteProcessGroup>() {

			@Override
			public int compare(RemoteProcessGroup o1, RemoteProcessGroup o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return list;
	}

	public static ArrayList<ProcessorNode> sortProcessorNodes(
			ArrayList<ProcessorNode> list) {
		Collections.sort(list, new Comparator<ProcessorNode>() {

			@Override
			public int compare(ProcessorNode o1, ProcessorNode o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return list;
	}

	public static ArrayList<Label> sortLabels(ArrayList<Label> list) {
		Collections.sort(list, new Comparator<Label>() {

			@Override
			public int compare(Label o1, Label o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});
		return list;
	}

	public static ArrayList<ProcessGroup> sortProcessGroups(
			ArrayList<ProcessGroup> list) {
		Collections.sort(list, new Comparator<ProcessGroup>() {

			@Override
			public int compare(ProcessGroup o1, ProcessGroup o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return list;
	}

}

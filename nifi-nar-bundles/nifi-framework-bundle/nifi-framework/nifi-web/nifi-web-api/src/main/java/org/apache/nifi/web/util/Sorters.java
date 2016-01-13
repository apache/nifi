package org.apache.nifi.web.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;

public class Sorters {

	public static NullSafeComparator<String> nullSafeStringComparator = new NullSafeComparator<String>();
	public static int nullSafeCompare(String o1, String o2) {
		return nullSafeStringComparator.compare(o1, o2);
	}

	public static List<Port> sortPorts(List<Port> list) {
		Collections.sort(list, new Comparator<Port>() {

			@Override
			public int compare(final Port o1, final Port o2) {
				String s1 = (null != o1)? o1.getName() : null;
				String s2 = (null != o2)? o2.getName() : null;

				return nullSafeCompare(s1, s2);
			}
		});

		return list;
	}

	public static List<RemoteProcessGroup> sortRemoteProcessGroups(List<RemoteProcessGroup> list) {
		Collections.sort(list, new Comparator<RemoteProcessGroup>() {

			@Override
			public int compare(RemoteProcessGroup o1, RemoteProcessGroup o2) {
				String s1 = (null != o1)? o1.getName() : null;
				String s2 = (null != o2)? o2.getName() : null;

				return nullSafeCompare(s1, s2);
			}
		});
		return list;
	}

	public static List<ProcessorNode> sortProcessorNodes(List<ProcessorNode> list) {
		Collections.sort(list, new Comparator<ProcessorNode>() {

			@Override
			public int compare(ProcessorNode o1, ProcessorNode o2) {
				String s1 = (null != o1)? o1.getName() : null;
				String s2 = (null != o2)? o2.getName() : null;

				return nullSafeCompare(s1, s2);
			}
		});
		return list;
	}

	public static List<Label> sortLabels(List<Label> list) {
		Collections.sort(list, new Comparator<Label>() {

			@Override
			public int compare(Label o1, Label o2) {
				String s1 = (null != o1)? o1.getValue() : null;
				String s2 = (null != o2)? o2.getValue() : null;

				return nullSafeCompare(s1, s2);
			}
		});
		return list;
	}

	public static List<ProcessGroup> sortProcessGroups(List<ProcessGroup> list) {
		Collections.sort(list, new Comparator<ProcessGroup>() {

			@Override
			public int compare(ProcessGroup o1, ProcessGroup o2) {
				String s1 = (null != o1)? o1.getName() : null;
				String s2 = (null != o2)? o2.getName() : null;

				return nullSafeCompare(s1, s2);
			}
		});
		return list;
	}

}

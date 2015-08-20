package org.apache.nifi.web.util;

import java.util.Comparator;

public class NullSafeComparator<E extends Comparable<E>> implements Comparator<E> {

	@Override
	public int compare(E o1, E o2) {
		if(null == o1 ^ null == o2) {
			return (o1 == null) ? -1 : 1;
		}

		if(null == o1 && null == o2) {
			return 0;
		}

		return o1.compareTo(o2);
	}
}

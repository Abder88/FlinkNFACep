package org.tests;

import java.util.Comparator;
import java.util.List;
 

 class ListEventComparator implements Comparator<List<Eventtest>> {

	@Override
	public int compare(List<Eventtest> o1, List<Eventtest> o2) {
		int sizeComp = Integer.compare(o1.size(), o2.size());
		if (sizeComp == 0) {
			EventComparator comp = new EventComparator();
			for (int i = 0; i < o1.size(); i++) {
				int eventComp = comp.compare(o1.get(i), o2.get(i));
				if (eventComp != 0) {
					return eventComp;
				}
			}
			return 0;
		} else {
			return sizeComp;
		}
	}
}

package org.tests;

import java.util.Comparator;


import com.google.common.primitives.Doubles;

class EventComparator implements Comparator<Eventtest> {

	@Override
	public int compare(Eventtest o1, Eventtest o2) {
		int nameComp = o1.getName().compareTo(o2.getName());
		int priceComp = Doubles.compare(o1.getPrice(), o2.getPrice());
		int idComp = Integer.compare(o1.getId(), o2.getId());
		if (nameComp == 0) {
			if (priceComp == 0) {
				return idComp;
			} else {
				return priceComp;
			}
		} else {
			return nameComp;
		}
	}
}
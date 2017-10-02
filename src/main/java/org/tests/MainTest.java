package org.tests;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MainTest {

	public static void main(String[] args) {
		testConditionNFA();

		System.out.println("dfsgdgfs");
		testNoConditionNFA();

	}

	public static void testNoConditionNFA() {

		List<StreamRecord<Eventtest>> inputEvents = new ArrayList<StreamRecord<Eventtest>>();

		Eventtest a = new Eventtest(40, "a", 1.0);
		Eventtest b = new Eventtest(41, "b", 2.0);
		Eventtest c = new Eventtest(42, "c", 3.0);
		Eventtest d = new Eventtest(43, "d", 4.0);
		Eventtest e = new Eventtest(44, "e", 5.0);

		inputEvents.add(new StreamRecord<Eventtest>(a, 1));
		inputEvents.add(new StreamRecord<Eventtest>(b, 2));
		inputEvents.add(new StreamRecord<Eventtest>(c, 3));
		inputEvents.add(new StreamRecord<Eventtest>(d, 4));
		inputEvents.add(new StreamRecord<Eventtest>(e, 5));

		Pattern<Eventtest, ?> pattern = Pattern.<Eventtest> begin("start").followedBy("end");

		NFA<Eventtest> nfa = NFACompiler.compile(pattern, Eventtest.createTypeSerializer(), false);
		//
		List<List<Eventtest>> resultingPatterns = feedNFA(inputEvents, nfa);
		System.out.println(resultingPatterns.get(0).toString());

		// System.out.println("jkbjkhjk");
		// compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
		// Lists.newArrayList(a, b),
		// Lists.newArrayList(b, c),
		// Lists.newArrayList(c, d),
		// Lists.newArrayList(d, e)
		// ));
	}

	static private List<List<Eventtest>> feedNFA(List<StreamRecord<Eventtest>> inputEvents, NFA<Eventtest> nfa) {
		List<List<Eventtest>> resultingPatterns = new ArrayList<>();

		for (StreamRecord<Eventtest> inputEvent : inputEvents) {
			Collection<Map<String, List<Eventtest>>> patterns = nfa.process(inputEvent.getValue(),
					inputEvent.getTimestamp()).f0;

			for (Map<String, List<Eventtest>> p : patterns) {
				List<Eventtest> res = new ArrayList<>();
				for (List<Eventtest> le : p.values()) {
					res.addAll(le);
				}
				resultingPatterns.add(res);
			}
		}
		return resultingPatterns;
	}
	//
	// private static void compareMaps(List<List<Event>> actual,
	// List<List<Event>> expected) {
	// Assert.assertEquals(expected.size(), actual.size());
	//
	// for (List<Event> p: actual) {
	// Collections.sort(p, new EventComparator());
	// }
	//
	// for (List<Event> p: expected) {
	// Collections.sort(p, new EventComparator());
	// }
	//
	// Collections.sort(actual, new ListEventComparator());
	// Collections.sort(expected, new ListEventComparator());
	// Assert.assertArrayEquals(expected.toArray(), actual.toArray());
	// }
	// public class ListEventComparator implements Comparator<List<Event>> {
	//
	// @Override
	// public int compare(List<Event> o1, List<Event> o2) {
	// int sizeComp = Integer.compare(o1.size(), o2.size());
	// if (sizeComp == 0) {
	// EventComparator comp = new EventComparator();
	// for (int i = 0; i < o1.size(); i++) {
	// int eventComp = comp.compare(o1.get(i), o2.get(i));
	// if (eventComp != 0) {
	// return eventComp;
	// }
	// }
	// return 0;
	// } else {
	// return sizeComp;
	// }
	// }
	// }
	//
	// public class EventComparator implements Comparator<Event> {
	//
	// @Override
	// public int compare(Event o1, Event o2) {
	// int nameComp = o1.getName().compareTo(o2.getName());
	// int priceComp = Double.compare(o1.getPrice(), o2.getPrice());
	// int idComp = Integer.compare(o1.getId(), o2.getId());
	// if (nameComp == 0) {
	// if (priceComp == 0) {
	// return idComp;
	// } else {
	// return priceComp;
	// }
	// } else {
	// return nameComp;
	// }
	// }
	// }

	public static void testConditionNFA() {

		List<StreamRecord<StockEvent>> StockEvents = new ArrayList<StreamRecord<StockEvent>>();

		StockEvent a = new StockEvent(0, 0, 1, 10, 100);
		StockEvent b = new StockEvent(1, 1, 1, 20, 90);
		StockEvent c = new StockEvent(2, 2, 1, 30, 80);
		StockEvent d = new StockEvent(3, 3, 1, 40, 70);
		StockEvent e = new StockEvent(4, 4, 1, 50, 60);
		StockEvent f = new StockEvent(5, 5, 1, 60, 50);
		StockEvent g = new StockEvent(6, 6, 1, 10, 40);

		StockEvents.add(new StreamRecord<StockEvent>(a, 1));
		StockEvents.add(new StreamRecord<StockEvent>(b, 2));
		StockEvents.add(new StreamRecord<StockEvent>(c, 3));
		StockEvents.add(new StreamRecord<StockEvent>(d, 4));
		StockEvents.add(new StreamRecord<StockEvent>(e, 5));
		StockEvents.add(new StreamRecord<StockEvent>(f, 6));
		StockEvents.add(new StreamRecord<StockEvent>(g, 7));
		Pattern<StockEvent, ?> pattern = Pattern.<StockEvent> begin("A").where(new SimpleCondition<StockEvent>() {
			@Override
			public boolean filter(StockEvent event) {
				System.out.println("A" + event.id);
				return true;
			}
		}).followedByAny("B").where(

				new IterativeCondition<StockEvent>() {
					private static final long serialVersionUID = -9216505110246259082L;

					@Override
					public boolean filter(StockEvent bEvent, Context<StockEvent> ctx) throws Exception {
						System.out.println("B" + bEvent.id);

						StockEvent lastb = null;
						Iterator<StockEvent> iteratorOverB = ctx.getEventsForPattern("B").iterator();
						while (iteratorOverB.hasNext()) {
							lastb = iteratorOverB.next();
							if (bEvent.getPrice() > lastb.getPrice() && bEvent.getVolume() < lastb.getVolume()) {
								return true;
							}
						}

						StockEvent aEvents = null;
						Iterator<StockEvent> iteratorOverA = ctx.getEventsForPattern("A").iterator();
						while (iteratorOverA.hasNext()) {
							aEvents = iteratorOverA.next();
							if (bEvent.getPrice() > aEvents.getPrice()) {
								return true;
							}
						}

						return false;
					}
				}).oneOrMore().allowCombinations().followedByAny("C").where(

						new IterativeCondition<StockEvent>() {
							private static final long serialVersionUID = -9216505110246259082L;

							@Override
							public boolean filter(StockEvent cEvent, Context<StockEvent> ctx) throws Exception {
								System.out.println("C" + cEvent.id);
								StockEvent bEvents = null;
								Iterator<StockEvent> iteratorOverB = ctx.getEventsForPattern("B").iterator();
								while (iteratorOverB.hasNext()) {
									bEvents = iteratorOverB.next();
									if (cEvent.getPrice() < bEvents.getPrice()) {
										return true;
									}
								}
								return false;
							}
						});

		NFA<StockEvent> nfa = NFACompiler.compile(pattern, StockEvent.createTypeSerializer(), false);
		//
		List<List<StockEvent>> resultingPatterns = feedNFAStockEvent(StockEvents, nfa);
		for (int i = 0; i < resultingPatterns.size(); i++) {

			for (int j = 0; j < resultingPatterns.get(i).size(); j++)
				// System.out.print( resultingPatterns.get(i).get(j).price +"
				// ,"+resultingPatterns.get(i).get(j).volume + " ; ");
				System.out.print(resultingPatterns.get(i).get(j).id + " ;  ");
			System.out.println();
		}
		System.out.println("fzefzfe" + resultingPatterns.get(0).toString());

		// System.out.println("jkbjkhjk");
		// compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
		// Lists.newArrayList(a, b),
		// Lists.newArrayList(b, c),
		// Lists.newArrayList(c, d),
		// Lists.newArrayList(d, e)
		// ));
	}

	private static List<List<StockEvent>> feedNFAStockEvent(List<StreamRecord<StockEvent>> stockEvents,
			NFA<StockEvent> nfa) {
		List<List<StockEvent>> resultingPatterns = new ArrayList<>();

		for (StreamRecord<StockEvent> inputEvent : stockEvents) {
			Collection<Map<String, List<StockEvent>>> patterns = nfa.process(inputEvent.getValue(),
					inputEvent.getTimestamp()).f0;

			for (Map<String, List<StockEvent>> p : patterns) {
				List<StockEvent> res = new ArrayList<>();
				for (List<StockEvent> le : p.values()) {
					res.addAll(le);
				}
				resultingPatterns.add(res);
			}
		}
		return resultingPatterns;
	}

}

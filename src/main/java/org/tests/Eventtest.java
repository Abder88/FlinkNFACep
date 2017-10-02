package org.tests;

import java.util.Objects;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;


public class Eventtest {
	private String name;
	private double price;
	private int id;

	public Eventtest(int id, String name, double price) {
		this.id = id;
		this.name = name;
		this.price = price;
	}

	public double getPrice() {
		return price;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return "Event(" + id + ", " + name + ", " + price + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Eventtest) {
			Eventtest other = (Eventtest) obj;

			return name.equals(other.name) && price == other.price && id == other.id;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, price, id);
	}

	public static TypeSerializer<Eventtest> createTypeSerializer() {
		TypeInformation<Eventtest> typeInformation = (TypeInformation<Eventtest>) TypeExtractor.createTypeInfo(Eventtest.class);

		return typeInformation.createSerializer(new ExecutionConfig());
	}
}

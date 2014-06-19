package nl.naward04.hadoop.beverages;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class BeverageWritable implements Writable {
	HashMap<String, Long> beverages = new HashMap<String, Long>();

	public BeverageWritable() {
	}

	public BeverageWritable(String val1, long val2) {
		beverages.put(val1, val2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		while (true) {
			try {
				String val1 = WritableUtils.readString(in);
				long val2 = in.readLong();
				beverages.put(val1, val2);
			} catch (Exception e) {
				//do nothing
				break;
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (String beverage : beverages.keySet()) {
			WritableUtils.writeString(out, beverage);
			out.writeLong(beverages.get(beverage));
		}
	}

	public void merge(BeverageWritable other) {
		for (String beverage : other.beverages.keySet()) {
			if(beverages.containsKey(beverage)) {
				beverages.put(beverage, beverages.get(beverage) + other.beverages.get(beverage));
			} else {
				beverages.put(beverage, other.beverages.get(beverage));
			}
		}
	}

	@Override
	public String toString() {
		String result = "\n";
		for (String beverage : beverages.keySet()) {
			result += beverage + " " + beverages.get(beverage) + "\n";
		}
		
		return result;
	}
}

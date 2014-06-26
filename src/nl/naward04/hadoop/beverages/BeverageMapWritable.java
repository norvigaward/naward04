package nl.naward04.hadoop.beverages;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * Class for keeping a HashMap, only function is to overwrite the toString method
 *
 */
public class BeverageMapWritable extends MapWritable {

	public String toString() {
		String result = "\n";
		
		for (Writable k : this.keySet()) {
			result += k + " " + this.get(k) + "\n";
		}
		
		return result;
	}
	
}

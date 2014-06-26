package nl.naward04.hadoop.beverages;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class BeverageCountReducer extends Reducer<Text, BeverageMapWritable, Text, BeverageMapWritable> {

	@Override
    public void reduce(Text key, Iterable<BeverageMapWritable> values, Context context)
            throws IOException, InterruptedException {
        
		BeverageMapWritable out = new BeverageMapWritable();

	    for (MapWritable next : values)
	    {
	    	for (Writable k : next.keySet()) {
    			long v2 = ((LongWritable)next.get(k)).get();
	    		if (out.containsKey(k)) {
	    			long v1 = ((LongWritable)out.get(k)).get();
	    			out.put(k, new LongWritable(v1+v2));
	    		} else {
	    			out.put(k, new LongWritable(v2));
	    		}
	    	}
	    }
		context.write(key, out);
    }
}
package nl.naward04.hadoop.beverages;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jwat.warc.WarcRecord;

public class BeverageCountReducer extends Reducer<Text, BeverageWritable, Text, BeverageWritable> {

	@Override
    public void reduce(Text key, Iterable<BeverageWritable> values, Context context)
            throws IOException, InterruptedException {
        
		BeverageWritable out = new BeverageWritable();

	    for (BeverageWritable next : values)
	    {
	        out.merge(next);
	    }
		context.write(key, out);
    }
}
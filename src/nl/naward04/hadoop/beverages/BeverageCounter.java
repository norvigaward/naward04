package nl.naward04.hadoop.beverages;

import nl.surfsara.warcutils.WarcInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

// Main Tool class for Counting the beverages
public class BeverageCounter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		Job job = Job.getInstance(conf, "BeverageCounter");
		job.setJarByClass(BeverageCounter.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(BeverageCountExtracter.class);
		job.setReducerClass(BeverageCountReducer.class);
		
		job.setInputFormatClass(WarcInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BeverageMapWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	
	
	
}

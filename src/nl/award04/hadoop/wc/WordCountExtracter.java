package nl.award04.hadoop.wc;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

public class WordCountExtracter extends Mapper<LongWritable, WarcRecord, Text, LongWritable> {
	private static final int MAX_RECORDS = 100;
	private long numRecords = 0;
	
	private static enum Counters {
		CURRENT_RECORD, NUM_WORDS
	}
	
	@Override
	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());
		
		if(numRecords < MAX_RECORDS) {
		
			if("text/plain".equals(value.header.contentTypeStr)){
				Payload payload = value.getPayload();
				if (payload == null) {
					//Do nothing
				} else {
					String wetContent = IOUtils.toString(payload.getInputStreamComplete());
					if(wetContent == null || "".equals(wetContent)){
						//DO nothing
					} else {
						int wc = (wetContent.split("\\s+")).length;
						context.write(new Text("wc"), new LongWritable(wc));
					}
						
				}
				numRecords++;
				
			}
		
		}
		
	}
	
	
	
}

package nl.naward04.hadoop.wc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;

public class WordCountExtracter extends Mapper<LongWritable, WarcRecord, Text, LongWritable> {

	private static final Logger logger = Logger.getLogger(WordCountExtracter.class);
	private static final int MAX_RECORDS = 50;
	private long numRecords = 0;
	private AbstractSequenceClassifier<CoreLabel> classifier;
	
	private static enum Counters {
		CURRENT_RECORD, NUM_WORDS
	}
	
	private HashMap<String, Integer> wordsToCount = new HashMap<String, Integer>();
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		wordsToCount.put("wodka", 0);
		wordsToCount.put("beer", 0);
		wordsToCount.put("wine", 0);
		wordsToCount.put("vodka", 0);
		wordsToCount.put("weizen", 0);
		wordsToCount.put("carslberg", 0);
		wordsToCount.put("heineken", 0);
		super.setup(context);
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
						Scanner sc = new Scanner(wetContent);
						while(sc.hasNext()){
							String word = sc.next().toLowerCase();
							if(wordsToCount.containsKey(word)){
								wordsToCount.put(word, (wordsToCount.get(word))+1);
							}
						}
						for(String word : wordsToCount.keySet()) {
							//if(wordsToCount.get(word) > 0) {
								context.write(new Text(word), new LongWritable(wordsToCount.get(word)));
							//}
						}
					}
						
				}
				numRecords++;
				
			}
		
		}
		
	}
	
	
	
}

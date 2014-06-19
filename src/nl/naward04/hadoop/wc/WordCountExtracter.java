package nl.naward04.hadoop.wc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import nl.naward04.wordtree.WordTree;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.dict.DictionaryEntry;
import com.aliasi.dict.ExactDictionaryChunker;
import com.aliasi.dict.MapDictionary;
import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory;

public class WordCountExtracter extends Mapper<LongWritable, WarcRecord, Text, LongWritable> {

	private static final int MAX_RECORDS = 50;
	private long numRecords = 0;
	
	static final int CHUNK_SCORE = 1;
	private MapDictionary<String> dictionary = new MapDictionary<String>();
	ExactDictionaryChunker wordDictionaryChunker;
	
	private static enum Counters {
		CURRENT_RECORD, NUM_WORDS
	}
	
	private WordTree wordsToCount;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("drinklist.csv")));		
		try {
			while(true) {
				String drink = reader.readLine();
				dictionary.addEntry(new DictionaryEntry<String>(drink, drink,CHUNK_SCORE));
			}
		} catch (IOException e) {
			//Probably end of file
		} catch (NullPointerException e) {
			//error reading file, probably end of file
		}
		
		wordDictionaryChunker = new ExactDictionaryChunker(dictionary, IndoEuropeanTokenizerFactory.INSTANCE, true, false);
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
					if (wetContent == null || "".equals(wetContent)) {
						//DO nothing
					} else {
						
						Chunking chunking = wordDictionaryChunker.chunk(wetContent);
						
						for (Chunk chunk : chunking.chunkSet()) {
							context.write(new Text(chunk.type()), new LongWritable((long)chunk.score()));
						}
						
//						
//						Scanner sc = new Scanner(wetContent);
//						while(sc.hasNext()){
//							String word = sc.next().toLowerCase();
//							if(wordsToCount.hasSentence(word)) { //containsKey(word)){
//								wordsToCount.put(word, (wordsToCount.get(word))+1);
//							}
//						}
//						for(String word : wordsToCount.keySet()) {
//							//if(wordsToCount.get(word) > 0) {
//								context.write(new Text(word), new LongWritable(wordsToCount.get(word)));
//							//}
//						}
					}
						
				}
				//numRecords++;
				
			}
		
		}
		
	}
	
	
	
}

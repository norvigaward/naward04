package nl.naward04.hadoop.beverages;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import nl.naward04.hadoop.country.AddressRangeList;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.dict.DictionaryEntry;
import com.aliasi.dict.ExactDictionaryChunker;
import com.aliasi.dict.MapDictionary;
import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory;

/**
 * Main Mapper class for counting beverages on a WebPage
 *
 * Takes as input a Key-value pair where the key is a LongWritable and the Value is a WarcRecord
 * Outputs a Key-value pair where the key is a countrycode string and the value is a BeverageMap (HashMap) with keys as beverage strings and the value a longwritable count
 */
public class BeverageCountExtracter extends Mapper<LongWritable, WarcRecord, Text, BeverageMapWritable> {

	private final Set<String> invalidTLDs = new TreeSet<String>();
	
	static final int CHUNK_SCORE = 1;
	private MapDictionary<String> dictionary = new MapDictionary<String>();
	ExactDictionaryChunker wordDictionaryChunker;
	
	private AddressRangeList list;
	
	private static enum Counters {
		CURRENT_RECORD, NUM_WORDS
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("drinklist.csv")));		
		try {
			while(true) {
				String drink = reader.readLine().toLowerCase();
				dictionary.addEntry(new DictionaryEntry<String>(drink, drink,CHUNK_SCORE));
			}
		} catch (IOException e) {
			//Probably end of file
		} catch (NullPointerException e) {
			//error reading file, probably end of file
		}
		
		wordDictionaryChunker = new ExactDictionaryChunker(dictionary, IndoEuropeanTokenizerFactory.INSTANCE, true, false);
		
		list = AddressRangeList.getInstance();

		// From: http://en.wikipedia.org/wiki/GccTLD
		// + ly + eu
		invalidTLDs.addAll(Arrays.asList(new String[] { "ad", "as", "bz", "cc",
				"cd", "cl", "dj", "eu", "fm", "io", "la", "ly", "me", "ms",
				"nu", "sc", "sr", "su", "tv", "tk", "ws" }));
		
		super.setup(context);
	}
	
	@Override
	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());
		
		try {
			//We are only interested in responses
			if ("application/http; msgtype=response".equals(value.header.contentTypeStr)) {
				HttpHeader httpHeader = value.getHttpHeader();
				if (httpHeader == null) {
					// No header so we are unsure that the content is text/html: NOP
				} else {
					if (httpHeader.contentType != null && httpHeader.contentType.contains("text/html")) {
						
						//First determine the country of origin
						boolean found = false;
						
						//Try via the hostname
						String country = null;
						String host = value.header.warcInetAddress.getHostName();
						if (host != null){	
							String tld = host.substring(host.lastIndexOf('.') + 1).toLowerCase();
							if (tld.length() == 2 && tld.matches("[a-z]{2}")){
								if(!invalidTLDs.contains(tld)) {
									country = tld;
									found = true;
								}
							}
						}
						
						//Else try via the ip address and geo location
						if (!found && country == null && value.header.warcIpAddress != null){
							String IP = value.header.warcIpAddress;
							if (IP != null) {
								country = list.getCountry(AddressRangeList.convertAddressToLong(IP)).toLowerCase();
							}
						}
						
						
						//We only care for some domains, further processing if domain is not desired is useless
						if (!found) {
							return;
						}
						
						//Then see which beverages are present
						Payload payload = value.getPayload();
						if (payload == null) {
							//Do nothing
						} else {
							String warcContent = IOUtils.toString(payload.getInputStreamComplete());
							if (warcContent == null || "".equals(warcContent)) {
								//DO nothing
							} else {
								Chunking chunking = wordDictionaryChunker.chunk(warcContent);
								
								for (Chunk chunk : chunking.chunkSet()) {
									BeverageMapWritable hashmap = new BeverageMapWritable();
									hashmap.put(new Text(chunk.type()) , new LongWritable((long)chunk.score()));
									context.write(new Text(country), hashmap);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			//Do nothing, if one webpage fails for some reason (nullpointer or something like that) we are not interrested
		}
	}
}




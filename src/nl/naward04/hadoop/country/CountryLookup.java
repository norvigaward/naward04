
package nl.naward04.hadoop.country;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

/**
 * Map function that from a WarcRecord (warc) parses the headers' content and extracts the country where the server resides. The resulting key, values: country, 1.  
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
class CountryLookup extends Mapper<LongWritable, WarcRecord, Text, Text> {
	private static final Logger logger = Logger.getLogger(CountryLookup.class);
	private final Set<String> invalidTLDs = new TreeSet<String>();
	private AddressRangeList list;

	private static enum Counters {
		CURRENT_RECORD, NUM_JSON_RECORDS
	}
	
	public CountryLookup() {
		// From: http://en.wikipedia.org/wiki/GccTLD
		// + ly + eu
		invalidTLDs.addAll(Arrays.asList(new String[] { "ad", "as", "bz", "cc",
				"cd", "cl", "dj", "eu", "fm", "io", "la", "ly", "me", "ms",
				"nu", "sc", "sr", "su", "tv", "tk", "ws" }));
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) {
		list = AddressRangeList.getInstance();
	}

	
	@Override
	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());

		// Only process http response content. Note that the outlinks can also be found in the wat metadata.
//		if ("application/http; msgtype=response".equals(value.header.contentTypeStr)) {
//			// org.jwat.warc.WarcRecord is kind enough to also parse http headers for us:
//			HttpHeader httpHeader = value.getHttpHeader();
//			if (httpHeader == null) {
//				// No header so we are unsure that the content is text/html: NOP
//			} else {
//				if (httpHeader.contentType != null && httpHeader.contentType.contains("text/html")) {
//					String country = null;
//					String host = value.header.warcInetAddress.getHostName();
//					if (host != null){	
//						String tld = host.substring(host.lastIndexOf('.') + 1);
//						if (tld.length() == 2 && tld.matches("[a-z]{2}")){
//							if(!invalidTLDs.contains(tld)) {
//								country = tld;
//							}
//						}
//					}
//					if (country == null && value.header.warcIpAddress!= null){
//						String IP = value.header.warcIpAddress;
//						country = list.getCountry(AddressRangeList.convertAddressToLong(IP));
//					}
//				}
//			}
//		}
		
context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());
		
		try {
			// Only try to parse json content
			if ("application/json".equals(value.header.contentTypeStr)) {
				context.getCounter(Counters.NUM_JSON_RECORDS).increment(1);
				// Get the json payload
				Payload payload = value.getPayload();
				if (payload == null) {
					// NOP
				} else {
					String warcContent = IOUtils.toString(payload.getInputStreamComplete());
					JSONObject json = new JSONObject(warcContent);

					String warc_type = json.getJSONObject("Envelope").getJSONObject("WARC-Header-Metadata").getString("WARC-Type");
					if(!warc_type.equals("response")) {
						// Not relevant, we only use responses
						return;
					}

					String warc_record_id = json.getJSONObject("Envelope").getJSONObject("WARC-Header-Metadata").getString("WARC-Record-ID");
					String country = null;

					if(value.header.warcTargetUriUri != null) {
						String host = value.header.warcTargetUriUri.getHost();
						String tld = host.substring(host.lastIndexOf('.') + 1);
						if (tld.length() == 2 && tld.matches("[a-z]{2}")){
							if(!invalidTLDs.contains(tld)) {
								country = tld;
							}
						}
					}

					if(country == null) {
						try {
							String IP = json.getJSONObject("Envelope").getJSONObject("WARC-Header-Metadata").getString("WARC-IP-Address");
							country = list.getCountry(AddressRangeList.convertAddressToLong(IP));
						} catch (JSONException e) {
							// Not the JSON we were looking for.. 
							logger.error(e);
						}
					}
					if (country != null) {
						context.write(new Text(warc_record_id), new Text("COUNTRY#" + country));
					}
				}
			}
		} catch(JSONException e) {
			logger.warn("Decoding JSON for key " + key.get() + " failed", e);
		}
		
	}
//	@Override
//	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
//		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());
//		
//
//		// Only try to parse json content
//		if ("application/json".equals(value.header.contentTypeStr)) {
//			context.getCounter(Counters.NUM_JSON_RECORDS).increment(1);
//			// Get the json payload
//			Payload payload = value.getPayload();
//			if (payload == null) {
//				// NOP
//			} else {
//				String country = null;
//				
//				if(value.header.warcTargetUriUri != null) {
//					String host = value.header.warcTargetUriUri.getHost();
//					String tld = host.substring(host.lastIndexOf('.') + 1);
//					if (tld.length() == 2 && tld.matches("[a-z]{2}")){
//						if(!invalidTLDs.contains(tld)) {
//							country = tld;
//						}
//					}
//				}
//				/*
//				if (country == null && value.header.warcIpAddress!= null){
//					String IP = value.header.warcIpAddress;
//					AddressRangeList list = AddressRangeList.getInstance();
//					country = list.getCountry(AddressRangeList.convertAddressToLong(IP));
//					
//				}
//				*/
//				if(country == null) {
//					String warcContent = IOUtils.toString(payload.getInputStreamComplete());
//					JSONObject json = new JSONObject(warcContent);
//					try {
//						String IP = json.getJSONObject("Envelope").getJSONObject("WARC-Header-Metadata").getString("WARC-IP-Address");
//						AddressRangeList list = AddressRangeList.getInstance();
//						country = list.getCountry(AddressRangeList.convertAddressToLong(IP));
//					} catch (JSONException e) {
//						// Not the JSON we were looking for.. 
//						logger.error(e);
//					}
//				}
//				if (country != null) {
//					context.write(new Text(country), new LongWritable(1));
//				}
//			}
//		}
//	}
}
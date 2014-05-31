
package nl.naward04.hadoop.country;

import java.io.IOException;

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
 * Map function that from a WarcRecord (wat) parses the JSON content and extracts the country where the server resides. The resulting key, values: country, 1.  
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
class CountryExtracter extends Mapper<LongWritable, WarcRecord, Text, LongWritable> {
	private static final Logger logger = Logger.getLogger(CountryExtracter.class);

	private static enum Counters {
		CURRENT_RECORD, NUM_JSON_RECORDS
	}

	@Override
	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());

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
				try {
					String IP = json.getJSONObject("Envelope").getJSONObject("WARC-Header-Metadata").getString("WARC-IP-Address");
					AddressRangeList list = AddressRangeList.getInstance();
					String country = list.getCountry(AddressRangeList.convertAddressToLong(IP));
					context.write(new Text(country), new LongWritable(1));
				} catch (JSONException e) {
					// Not the JSON we were looking for.. 
					logger.error(e);
				}
			}
		}
	}
}
package nl.naward04.junit;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import nl.naward04.hadoop.country.*;
import static org.junit.Assert.assertEquals;

public class TestAddressLookup {
	
	@Test
	public void lookup(){
		String address = "130.89.128.153";
		Long longAddress = AddressRangeList.convertAddressToLong(address);
		AddressRangeList list = AddressRangeList.getInstance();
		String country = list.getCountry(longAddress);
		assertEquals(country,"NL");
		System.out.println("Land gevonden: " + country); 
	}

}

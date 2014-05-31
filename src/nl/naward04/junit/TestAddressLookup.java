package nl.naward04.junit;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import nl.naward04.hadoop.country.*;
import static org.junit.Assert.assertEquals;

public class TestAddressLookup {
	
	@Test
	public void testValid(){
		String address = "130.89.128.153";
		Long longAddress = AddressRangeList.convertAddressToLong(address);
		AddressRangeList list = AddressRangeList.getInstance();
		String country = list.getCountry(longAddress);
		assertEquals("NL",country);
		System.out.println("Land gevonden: " + country); 
	}

	@Test
	public void testMissing(){
		String address = "24.152.100.100";
		Long longAddress = AddressRangeList.convertAddressToLong(address);
		AddressRangeList list = AddressRangeList.getInstance();
		String country = list.getCountry(longAddress);
		assertEquals("Unknown",country);
	}
	
	@Test
	public void testOutOfRange(){
		String address = "257.258.300.199";
		Long longAddress = AddressRangeList.convertAddressToLong(address);
		AddressRangeList list = AddressRangeList.getInstance();
		String country = list.getCountry(longAddress);
		assertEquals("Unknown",country);
	}
}

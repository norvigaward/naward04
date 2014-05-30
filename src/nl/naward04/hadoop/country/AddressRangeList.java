package nl.naward04.hadoop.country;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;


public class AddressRangeList {
	
	private static AddressRangeList instance;
	
	private AddressRange[] list;
	
	private AddressRangeList(){
		populate();
	}
	
	public static AddressRangeList getInstance(){
		if (instance == null){
			instance = new AddressRangeList();
		}
		return instance;
	}
	
	private void populate(){
		Scanner s = null;
		File f = new File("iplist.csv");
		try{
			s = new Scanner(f);
		} catch (FileNotFoundException e){
			e.printStackTrace();
		}
		ArrayList<AddressRange>  temp= new ArrayList<AddressRange> (100000);
		while (s.hasNextLine()){
			String[] split = s.nextLine().split(";");
			temp.add(new AddressRange(Long.parseLong(split[0]),Long.parseLong(split[1]), split[2]));
		}
		list = new AddressRange[temp.size()];
		list = temp.toArray(list);
		s.close();
	}
	
	public String getCountry(long address){
		int lower = 0;
		int upper = list.length-1;
		String ret = "Unknown";
		int middle = (lower + upper)/2;
		
		while( lower <= upper ){
	      if ( list[middle].compareTo(address) > 0 )
	        lower = middle + 1;    
	      else if ( list[middle].contains(address)) 
	      {
	       ret = list[middle].getCountry();
	       break;
	      }
	      else {
	    	  upper = middle - 1;
	    	  middle = (lower + upper)/2;
	      }
		}
		return ret;
	}
	
	public static long convertAddressToLong(String address){
		String[] addr = address.split("\\.");
		long ret=0;
		ret=ret+ Long.parseLong(addr[0]) * 16777216;
		ret=ret+ Long.parseLong(addr[1]) * 65536;
		ret=ret+ Long.parseLong(addr[2]) * 256;
		ret=ret+ Long.parseLong(addr[3]);
		return ret;
	}
}

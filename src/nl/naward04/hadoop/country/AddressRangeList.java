package nl.naward04.hadoop.country;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;


public class AddressRangeList {
	
	private static AddressRangeList instance;
	private AddressRange[] list;
	
	private AddressRangeList(){
		AddressRangeList ret = new AddressRangeList();
		ret.populate();
	}
	
	public AddressRangeList getInstance(){
		if (instance == null){
			instance = new AddressRangeList();
		}
		return instance;
	}
	
	private void populate(){
		Scanner s;
		s = new Scanner("iplist.csv");
		ArrayList<AddressRange>  temp= new ArrayList<AddressRange> (100000);
		while (s.hasNextLine()){
			String[] split = s.nextLine().split(";");
			temp.add(new AddressRange(Integer.parseInt(split[0]),Integer.parseInt(split[1]), split[2]));
		}
		list = (AddressRange[]) temp.toArray();
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
		
		
		return ret;
	}

}

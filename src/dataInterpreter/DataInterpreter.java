package dataInterpreter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * Interpreter for the result of the beverage counter. Requires two ;-delimted CSV files to convert the results to the format:
 * CN - CATEGORY - ENGLISHNAME - COUNT
 * Outputs result.csv, ;-delimited.
 * @author Marc Hulsebosch and Stijn van Winsen
 *
 */
public class DataInterpreter {
	
	private HashMap<String,String> toCategories = null;
	private HashMap<String,String> toEnglish = null;
	private HashMap<String,Long> totals = null;
	private PrintWriter out = null;
	private HashMap<String,HashMap<String,Long>> allDrinks = null;
	private HashMap<String,Long> beer = null;
	private HashMap<String,Long> wine = null;
	private HashMap<String,Long> liquor = null;
	private LinkedList<String> countries = null;
	
	public static void main(String[] args){
		new DataInterpreter().run(args[0]);
	}
	
	public DataInterpreter(){
		Path currentRelativePath = Paths.get("");
		String s = currentRelativePath.toAbsolutePath().toString();
		System.out.println("Current relative path is: " + s);
		populateEnglish();
		populateCategories();
		
		totals = new HashMap<String,Long>();
		
		beer = new HashMap<String,Long>();
		wine = new HashMap<String,Long>();
		liquor = new HashMap<String,Long>();
		allDrinks = new HashMap<String,HashMap<String,Long>>();
		allDrinks.put("beer",beer);
		allDrinks.put("wine",wine);
		allDrinks.put("liquor",liquor);
		countries = new LinkedList<String>();
	}
	
	private void makeFile(String file) {
			try {
				out = new PrintWriter(new File(file),"UTF-8");
			} catch (FileNotFoundException e) {
				System.out.println("File " + file + " could not be created");
			} catch (UnsupportedEncodingException e){
				System.out.println("UTF-8 not supported");
				e.printStackTrace();
			}
		
	}

	private void populateEnglish() {
		toEnglish = new HashMap<String,String>();
		try {
			Scanner sc = new Scanner(new FileInputStream("toenglish.csv"),"UTF-8");
			while(sc.hasNextLine()){
				String line = sc.nextLine();
				String[] values = line.split(";");
				for(int i=1;i<values.length;i++){
					toEnglish.put(values[i],values[0]);
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("toenglish.csv not found.");
		} catch (ArrayIndexOutOfBoundsException e){
			System.out.println("toenglish.csv is not valid.");
		}
		
	}
	
	private void populateCategories() {
		toCategories  = new HashMap<String,String>();
		try {
			Scanner sc = new Scanner(new FileInputStream("tocategory.csv"),"UTF-8");
			while(sc.hasNextLine()){
				String line = sc.nextLine();
				String[] values = line.split(";");
				toCategories.put(values[1],values[0]);
			}
		} catch (FileNotFoundException e) {
			System.out.println("tocategory not found.");
		} catch (ArrayIndexOutOfBoundsException e){
			System.out.println("tocategory is not valid.");
		}
	}

	


	public void run(String file) {
		long i = 0;
		BufferedReader br = null;
		makeFile("results.csv");
		String line = null;
		try{
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));
			String country = null;
			
			while((line = bufferedReader.readLine())!=null){
				if (line.endsWith("\t")) {
					line = line.substring(0,line.length()-1);
				}
				if(line.length() == 2){
					country = line;
					countries.add(country);
					beer.put(country,(long)0);
					wine.put(country,(long)0);
					liquor.put(country,(long)0);
				} else if (line.split(" ").length == 2){
					String cat = toCategory(line.split("\\s+")[0]);
					String eng = toEnglish(line.split("\\s+")[0]);
					if (cat == null || eng == null){
						//System.out.println("Error parsing line: \n"+line);
					}
					write(country+";"+cat+";"+eng+";"+line.split("\\s+")[1]);
					if(cat != null ){
						try{
							Long current = allDrinks.get(cat).get(country);
							allDrinks.get(cat).put(country, current + Long.parseLong(line.split("\\s+")[1]));
						} catch (NumberFormatException e){
							System.out.println("Invalid number ignored");
						}
					}
				}
				i++;
			}
		} catch (FileNotFoundException e) {
			System.out.println(file + " not found.");
		} catch (IOException e){
			System.out.println("End Of File reached");
		}
		out.close();
		System.out.println("Gearriveerd bij totals, aantal iteraties: "+i+"\nlaatste regel: \n"+line);
		makeFile("totals.csv");
		
		for (int cn = 0; cn < countries.size();cn++){
			String country = countries.get(cn);
			long beerTotal = beer.get(country);
			long wineTotal = wine.get(country);
			long liquorTotal = liquor.get(country);
			long total = beerTotal + wineTotal + liquorTotal;
			write(country+ ";" + beerTotal+ ";" + wineTotal+ ";" + liquorTotal + ";" + total);
			
		}
		out.close();
	}

	private void write(String string) {
		out.println(string);		
	}
	
	public String toCategory(String word){
		return toCategories.get(toEnglish(word));
	}

	private String toEnglish(String word) {
		return toEnglish.get(word);
	}
	

	
}

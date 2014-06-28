package dataInterpreter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
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
	private PrintWriter out = null;
	
	public static void main(String[] args){
		new DataInterpreter().run(args[0]);
	}
	
	public DataInterpreter(){
		Path currentRelativePath = Paths.get("");
		String s = currentRelativePath.toAbsolutePath().toString();
		System.out.println("Current relative path is: " + s);
		populateEnglish();
		populateCategories();
		makeFile();
	}
	
	private void makeFile() {
			try {
				out = new PrintWriter("results.csv");
			} catch (FileNotFoundException e) {
				System.out.println("Results file could not be created");
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
		Scanner sc = null;
		try{
			sc = new Scanner(new FileInputStream(file),"UTF-8");
			String country = null;
			while(sc.hasNextLine()){
				String line = sc.nextLine();
				if (line.endsWith("\t")) {
					line = line.substring(0,line.length()-1);
				}
				if(line.length() == 2){
					country = line;
				} else if (line.split("\\s+").length == 2){
					String cat = toCategory(line.split("\\s+")[0]);
					String eng = toEnglish(line.split("\\s+")[0]);
					if (cat == null || eng == null){
						System.out.println("Error parsing line: \n"+line);
					}
					write(country+";"+cat+";"+eng+";"+line.split("\\s+")[1]);
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println(file + " not found.");
		}
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

package interpreter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
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
		populateEnglish();
		populateCategories();
		makeFile();
	}
	
	private void makeFile() {
			try {
				out = new PrintWriter("results"+ new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) +".csv");
			} catch (FileNotFoundException e) {
				System.out.println("Results file could not be creatd");
			}
		
	}

	private void populateCategories() {
		toCategories = new HashMap<String,String>();
		try {
			Scanner sc = new Scanner(new File("tocategory.csv"));
			while(sc.hasNextLine()){
				String line = sc.nextLine();
				String[] values = line.split(";");
				for(int i=1;i<values.length;i++){
					toCategories.put(values[i],values[0]);
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("tocategory.csv not found.");
		} catch (ArrayIndexOutOfBoundsException e){
			System.out.println("tocategory.csv is not valid.");
		}
		
	}
	
	private void populateEnglish() {
		toEnglish  = new HashMap<String,String>();
		try {
			Scanner sc = new Scanner(new File("toenglish.csv"));
			while(sc.hasNextLine()){
				String line = sc.nextLine();
				String[] values = line.split(";");
				toEnglish.put(values[1],values[0]);
			}
		} catch (FileNotFoundException e) {
			System.out.println("toenglish not found.");
		} catch (ArrayIndexOutOfBoundsException e){
			System.out.println("toenglish is not valid.");
		}
	}

	


	public void run(String file) {
		Scanner sc = null;
		try{
			sc = new Scanner(new File(file));
			String country = null;
			while(sc.hasNextLine()){
				String line = sc.nextLine();
				if(line.length() == 2){
					country = line;
				} else if (line.split("\\s+").length ==2){
					write(country+";"+toCategory(line.split("\\s+")[0])+";"+toEnglish(line.split("\\s+")[0])+";"+line.split("\\s+")[1]);
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println(file + " not found.");
		}
	}

	private void write(String string) {
		out.write(string);
		
	}
	
	public String toCategory(String word){
		return toCategories.get(toEnglish(word));
	}

	private String toEnglish(String word) {
		return toEnglish.get(word);
	}
	

}

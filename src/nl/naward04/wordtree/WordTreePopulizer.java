package nl.naward04.wordtree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

public class WordTreePopulizer {

	private WordTree wordTree = new WordTree();
	
	ArrayList<String> sentences = new ArrayList<String>();
	
	public WordTreePopulizer(){
		sentences.add("bier");
		sentences.add("bier dingen");
		sentences.add("bier raad");
		sentences.add("bier dingen fiets");
		sentences.add("bier korting afstand lopen");
		sentences.add("bier wijn");
		sentences.add("gist");
		sentences.add("korting met fietsen");
		
		//populateWordTree(sentences);
		populateWordTreeWithBeverages("en");
		printWordTree();
	}
	
	public void populateWordTree(ArrayList<String> sentences2) {
		for(String sentence : sentences2) {
			wordTree.addSentence(sentence);
		}
	}
	
	public void populateWordTreeWithBeverages(String language){
		BufferedReader reader;
		try {
			reader  = new BufferedReader(new FileReader("nl/naward04/wordtree/resources/beverage-list-" + language + ".txt"));
		} catch (FileNotFoundException e) {
			System.out.println("Problem trying to read file: " + e.getMessage());
			return;
		}
		
		try {
			while(true) {
				wordTree.addSentence(reader.readLine());
			}
		} catch (IOException e) {
			//Probably end of file
		}
		
	}
	
	public void printWordTree(){
		wordTree.printTree();
	}
}

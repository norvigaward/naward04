package nl.naward04.wordtree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class WordTreePopulizer {

	private WordTree wordTree = new WordTree();
	
	public WordTreePopulizer(){
		populateWordTreeWithBeverages("en");
	}
	
	public WordTree sharedInstance() {
		return wordTree;
	}
	
	public void populateWordTree(ArrayList<String> sentences2) {
		for(String sentence : sentences2) {
			wordTree.addSentence(sentence);
		}
	}
	
	private void populateWordTreeWithBeverages(String language){
		BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("drinklist.csv")));		
		try {
			while(true) {
				wordTree.addSentence(reader.readLine());
			}
		} catch (IOException e) {
			//Probably end of file
		} catch (NullPointerException e) {
			//error reading file, probably end of file
		}
	}
	
	public void printWordTree(){
		wordTree.printTree();
	}
}

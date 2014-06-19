package nl.naward04.wordtree;

import nl.naward04.wordtree.Tree.Node;

public class WordTree {
	private Tree<KeyValuePair<String, Integer>> wordTree;
	
	public WordTree() {
		wordTree = new Tree<KeyValuePair<String, Integer>>(new KeyValuePair<String, Integer>("", 0));
	}
	
	public boolean hasSentence(String sentence) {
		Node<KeyValuePair<String, Integer>> currentNode = wordTree.getRoot();
		for(String word : sentence.split("\\s+")) {
			KeyValuePair<String, Integer> k = new KeyValuePair<String, Integer>(word, 0);
			if(currentNode.hasNextObject(k)) {
				currentNode = currentNode.getNextNodeFromObject(k);
				continue;
			} else {
				return false;
			}
		}
		return currentNode.isLeafNode();
	}
	
	public Node<KeyValuePair<String, Integer>> getLeafNodeOnSentence(String sentence) {
		Node<KeyValuePair<String, Integer>> currentNode = wordTree.getRoot();
		for(String word : sentence.split("\\s+")) {
			KeyValuePair<String, Integer> k = new KeyValuePair<String, Integer>(word, 0);
			if(currentNode.hasNextObject(k)) {
				currentNode = currentNode.getNextNodeFromObject(k);
				continue;
			} else {
				return null;
			}
		}
		if (currentNode.isLeafNode()) {
			return currentNode;
		}
		return null;
	}
	
	public void addSentence(String sentence) {
		Node<KeyValuePair<String, Integer>> currentNode = wordTree.getRoot();
		String[] sentenceInArray = sentence.split("\\s+");
		int wordCounter = 0;
		for(String word : sentenceInArray) {
			KeyValuePair<String, Integer> k = new KeyValuePair<String, Integer>(word, 0);
			if(currentNode.hasNextObject(k)) {
				currentNode = currentNode.getNextNodeFromObject(k);
			} else {
				Node<KeyValuePair<String, Integer>> newNode;
				if(wordCounter == sentenceInArray.length-1) {
					newNode = new Node<KeyValuePair<String, Integer>>(k, true);
				} else {
					newNode = new Node<KeyValuePair<String, Integer>>(k, false);
				}
				currentNode.addChild(newNode);
				currentNode = newNode;
			}
			wordCounter++;
		}
	}
	
	public void printTree() {
		wordTree.printTree();
	}
	
	
}

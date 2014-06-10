package nl.naward04.wordtree;

import java.util.ArrayList;
import java.util.List;

public class Tree<T> {
    private Node<T> root;

    public Tree() {
    	root = new Node<T>();
    }
    
    public Tree(T rootData) {
        root = new Node<T>();
        root.data = rootData;
    }
    
    public Node<T> getRoot() {
    	return root;
    }
    
    public void printTree() {
    	root.printNode();
    }

    public static class Node<T> {
        private T data;
        private Node<T> parent;
        private List<Node<T>> children;
        
        private boolean isLeafNode;
        
        public T getData() {
        	return data;
        }
        
        public Node() {
        	children = new ArrayList<Node<T>>();
        }
        
        public Node(T t, boolean isLeafNode) {
        	data = t;
        	this.isLeafNode = isLeafNode;
        	children = new ArrayList<Node<T>>();
        }
        
        public boolean isLeafNode() {
        	return this.isLeafNode;
        }
        
        public void addChild(Node<T> child) {
        	children.add(child);
        }
        
        public boolean hasNextObject(T object) {
        	for(Node<T> childNode : children) {
        		if(childNode.data.equals(object)) {
        			return true;
        		}
        	}
        	return false;
        }
        
        public Node<T> getNextNodeFromObject(T object) {
        	for(Node<T> childNode : children) {
        		if(childNode.data.equals(object)) {
        			return childNode;
        		}
        	}
        	return null;
        }
        
        public void printNode() {
        	System.out.println("" + data.toString() + " " + isLeafNode() + " {");
        	for(Node<T> node : children) {
        		node.printNode();
        	}
        	System.out.println("}");
        }
        
    }
}
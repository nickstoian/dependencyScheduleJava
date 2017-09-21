//Nicolas Stoian

import java.util.Scanner;

public class HashTable {
	private TableNode[] pointerArray;
	private int numNodes;

	public HashTable(int numNodes, Scanner inFile){
	    this.numNodes = numNodes;
	    pointerArray = new TableNode [numNodes];
	    for(int i = 0; i < numNodes; i++){
	        pointerArray[i] = new TableNode();
	    }
	    int n1;
	    int n2;
	    while(inFile.hasNext()){
	    	n1 = inFile.nextInt();
	    	n2 = inFile.nextInt();
	        insertTableNode(n1, n2);
	    }
	}

	public void insertTableNode(int nodeToAdd, int index){
	    TableNode walker = pointerArray[index - 1];
	    while(walker.getNext() != null){
	        walker = walker.getNext();
	    }
	    walker.setNext(new TableNode(nodeToAdd, null));
	}

	public TableNode elementAt(int index){
	    return pointerArray[index];
	}

	public boolean isDependentOn(int index, int node){
	    TableNode walker = pointerArray[index];
	    while(walker.getNext() != null){
	        if(walker.getNext().getJobId() == node){
	            return true;
	        }
	        walker = walker.getNext();
	    }
	    return false;
	}

	public int getNumNodes(){
	    return numNodes;
	}





}

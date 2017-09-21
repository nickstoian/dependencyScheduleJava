//Nicolas Stoian

public class LinkedList {
	private ListNode listHead;

	public LinkedList(){
	    listHead = new ListNode();
	}

	public ListNode getListHead(){
	    return listHead;
	}

	public void insertListNode(ListNode nodeToInsert){
	    ListNode walker = listHead;
	    while(walker.getNext() != null){
	        walker = walker.getNext();
	    }
	    walker.setNext(nodeToInsert);
	}

	public boolean isEmpty(){
	    if(listHead.getNext() == null){
	        return true;
	    }
	    else{
	        return false;
	    }
	}

	public ListNode removeListNode(){
	    ListNode toReturn = listHead.getNext();
	    listHead.setNext(toReturn.getNext());
	    toReturn.setNext(null);
	    return toReturn;
	}
}

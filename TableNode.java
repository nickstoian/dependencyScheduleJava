//Nicolas Stoian

public class TableNode {
	private int jobId;
	private TableNode next;

	public TableNode(){
		jobId = -1;
		next = null;
	}

	public TableNode(int jobId, TableNode next){
		this.jobId = jobId;
		this.next = next;
	}

	public int getJobId(){
	    return jobId;
	}

	TableNode getNext(){
	    return next;
	}

	void setNext(TableNode next){
	    this.next = next;
	}
}

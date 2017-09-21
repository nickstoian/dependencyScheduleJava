//Nicolas Stoian

public class ListNode {
	private int jobId;
	private int time;
	private ListNode next;

	public ListNode(){
		jobId = -1;
		time = -1;
		next = null;
	}

	public ListNode(int jobId, int time){
		this.jobId = jobId;
		this.time = time;
		next = null;
	}

	public int getJobId(){
	    return jobId;
	}

	public int getTime(){
	    return time;
	}

	public ListNode getNext(){
	    return next;
	}

	public void setNext(ListNode next){
	    this.next = next;
	}
}

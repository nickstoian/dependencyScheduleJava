//Nicolas Stoian

//This program needs 4 command line arguments
//args[0] "input1" for text file representing the data dependency pairs
//args[1] "input2" for text file representing the data job times
//args[2] "input3" for integer representing the number of processors
//args[3] "output1" to write the schedule table

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class DependencySchedule {
	public static void main(String args[]){
		try{
			Scanner inFile = new Scanner(new FileReader(args[0]));
			int numNodes;
		    numNodes = inFile.nextInt();
		    HashTable hashTable = new HashTable(numNodes, inFile);
		    Scanner inFile2 = new Scanner(new FileReader(args[1]));
		    int numNodes2;
		    numNodes2 = inFile2.nextInt();
		    if(numNodes != numNodes2){
		    	System.out.println("The number of nodes in the input files do not match, please check the arguments and try again");
		    	inFile.close();
			    inFile2.close();
		        return;
		    }
		    int procNeed = Integer.parseInt(args[2]);
		    if(procNeed > numNodes){
		        procNeed = numNodes;
		    }
		    int[] jobTime = new int [numNodes];
		    int totalJobTimes = 0;
		    int n1;
		    int n2;
		    while(inFile2.hasNext()){
		    	n1 = inFile2.nextInt();
		    	n2 = inFile2.nextInt();
		        jobTime[n1 - 1] = n2;
		        totalJobTimes += n2;
		    }

		    int[][] scheduleTable = new int[numNodes][totalJobTimes];
		    for(int row = 0; row < numNodes; row++){
		        for(int col = 0; col < totalJobTimes; col++){
		            scheduleTable[row][col] = -1;
		        }
		    }
		    LinkedList open = new LinkedList();
		    int[] processJob = new int [numNodes];
		    int[] processTime = new int [numNodes];
		    for(int i = 0; i < procNeed; i++){
		        processJob[i] = 0;
		        processTime[i] = 0;
		    }
		    int[] parentCount = new int [numNodes];
		    int[] jobDone = new int [numNodes];
		    int[] jobMarked = new int [numNodes];
		    for(int i = 0; i < numNodes; i++){
		        parentCount[i] = 0;
		        jobDone[i] = 0;
		        jobMarked[i] = 0;
		    }
		    int[] procUsed = new int[1];
		    procUsed[0] = 0;
		    int[] time = new int[1];
		    time[0] = 0;
		    loadParentCount(hashTable, parentCount, numNodes);
		    buildScheduleTable(scheduleTable, hashTable, open, processJob, processTime, parentCount, jobTime, jobDone, jobMarked, numNodes, procNeed, procUsed, time, totalJobTimes);
		    PrintWriter outFile = new PrintWriter(new FileWriter(args[3]));
		    outFile.println("Input 1 data dependency pairs");
		    outputHashTable(hashTable, outFile);
		    outFile.println();
		    outFile.println("Input 2 data job times");
		    outputJobTime(jobTime, numNodes, outFile);
		    outFile.println();
		    outFile.println("Number of processors = " + procNeed);
		    outFile.println();
		    outputScheduleTable(scheduleTable, time, procNeed, outFile);
		    inFile.close();
		    inFile2.close();
		    outFile.close();
		}
		catch(NoSuchElementException e){
			System.err.println("Error in input file format, check the input file and try again.");
            return;
		}
		catch(FileNotFoundException e){
			System.err.println("File not found exception, check arguements and try again.");
            return;
		}
		catch(IOException e){
			System.err.println("IO exception, check arguements and try again.");
            return;
		}
	}

	public static void loadParentCount(HashTable hashTable, int[] parentCount, int numNodes){
	    TableNode walker;
	    for(int i = 0; i < numNodes; i++){
	        walker = hashTable.elementAt(i);
	        int numParents = 0;
	        while(walker.getNext() != null){
	            numParents++;
	            walker = walker.getNext();
	        }
	        parentCount[i] = numParents;
	    }
	}

	public static void buildScheduleTable(int[][] scheduleTable, HashTable hashTable, LinkedList open, int[] processJob, int[] processTime, int[] parentCount,
            int[] jobTime, int[] jobDone, int[] jobMarked, int numNodes, int procNeed, int[] procUsed, int[] time, int totalJobTimes){
		while(!isGraphEmpty(jobDone, numNodes)){    //Step 11
	        findOrphenNodes(parentCount, jobMarked, jobTime, open, numNodes);   //Step 1
	        while(!open.isEmpty() && !(procUsed[0] >= procNeed)){     //Step 3
	            int availProc = -1; // Step 2 Start
	            for(int i = 0; i <= procNeed; i++){
	                if(processJob[i] <= 0){
	                    availProc = i;
	                    break;
	                }
	            }
	            if(availProc >= 0){
	                ListNode newJob = open.removeListNode();
	                processJob[availProc] = newJob.getJobId();
	                processTime[availProc] = newJob.getTime();
	                for(int i = time[0]; i < time[0] + newJob.getTime(); i++){
	                    scheduleTable[availProc][i] = newJob.getJobId();
	                }
	                procUsed[0]++;
	            }
	        }   // Step 2 End
	        if(open.isEmpty() && !isGraphEmpty(jobDone, numNodes) && isProcessJobDone(processJob, procNeed)){ // Step 4
	            System.out.println("Error, cycle detected in the graph. Program exiting.");
	            System.out.println();
	            System.exit(1);
	        }
	        printToConsole(scheduleTable, hashTable, open, processJob, processTime, parentCount,
	                       jobTime, jobDone, jobMarked, numNodes, procNeed, procUsed, time, totalJobTimes);  // Step 5
	        time[0]++; // Step 6
	        for(int i = 0; i < procNeed; i++){  // Step 7
	            processTime[i]--;
	        }
	        for(int i = 0; i < procNeed; i++){ // Step 9
	            if(processTime[i] == 0){    // Step 8 Start
	                int job = processJob[i] - 1;
	                processJob[i] = 0;
	                jobDone[job] = 1;
	                for(int j = 0; j < numNodes; j++){
	                    if(hashTable.isDependentOn(j, job + 1)){
	                        parentCount[j]--;
	                    }
	                }
	                procUsed[0]--;
	            }
	        } // Step 8 End
	        printToConsole(scheduleTable, hashTable, open, processJob, processTime, parentCount,
	                       jobTime, jobDone, jobMarked, numNodes, procNeed, procUsed, time, totalJobTimes); // Step 10
	    }
	}

	public static boolean isGraphEmpty(int[] jobDone, int numNodes){
	    for(int i = 0; i < numNodes; i++){
	        if(jobDone[i] == 0){
	            return false;
	        }
	    }
	    return true;
	}

	public static void findOrphenNodes(int[] parentCount, int[] jobMarked, int[] jobTime, LinkedList open, int numNodes){
	    for(int i = 0; i < numNodes; i++){
	        if(parentCount[i] == 0 && jobMarked[i] == 0){
	            jobMarked[i] = 1;
	            ListNode nodeToInsert = new ListNode(i + 1, jobTime[i]);
	            open.insertListNode(nodeToInsert);
	        }
	    }
	}

	public static boolean isProcessJobDone(int[] processJob, int procNeed){
	    for(int i = 0; i < procNeed; i++){
	        if(processJob[i] > 0){
	            return false;
	        }
	    }
	    return true;
	}

	public static void printToConsole(int[][] scheduleTable, HashTable hashTable, LinkedList open, int[] processJob, int[] processTime, int[] parentCount,
            int[] jobTime, int[] jobDone, int[] jobMarked, int numNodes, int procNeed, int[] procUsed, int[] time, int totalJobTimes){
		System.out.println("scheduleTable");
		System.out.format("%5s", "index");
	    for(int col = 0; col < time[0]; col++){
	        String t =  "t" + (col + 1);
	        System.out.format("%4s", t);
	    }
	    System.out.println();
	    for(int row = 0; row < procNeed; row++){
	        String p = "p" + (row + 1);
	        System.out.format("%5s", p);
	        for(int col = 0; col < time[0]; col++){
	            if(scheduleTable[row][col] == -1){
	            	System.out.format("%4s", "-");
	            }
	            else{
	            	System.out.format("%4s", scheduleTable[row][col]);
	            }
	        }
	        System.out.println();
	    }
	    System.out.println();
	    System.out.println("open list");
	    System.out.print("listHead -->");
	    ListNode walker = open.getListHead();
	    while(walker.getNext() != null){
	        System.out.print("(" + walker.getNext().getJobId() + "," + walker.getNext().getTime() + ") -->");
	        walker = walker.getNext();
	    }
	    System.out.print("NULL");
	    System.out.println();
	    System.out.println();
	    System.out.println("time == " + time[0]);
	    System.out.println("procUsed == " + procUsed[0]);
	    System.out.println();
	    System.out.println("processJob");
	    System.out.format("%5s", "index");
	    for(int i = 0; i < procNeed; i++){
	    	System.out.format("%3s", i);
	    }
	    System.out.println();
	    System.out.format("%5s", " ");
	    for(int i = 0; i < procNeed; i++){
	    	System.out.format("%3s", processJob[i]);
	    }
	    System.out.println();
	    System.out.println();
	    System.out.println("processTime");
	    System.out.format("%5s", "index");
	    for(int i = 0; i < procNeed; i++){
	    	System.out.format("%3s", i);
	    }
	    System.out.println();
	    System.out.format("%5s", " ");
	    for(int i = 0; i < procNeed; i++){
	    	System.out.format("%3s", processTime[i]);
	    }
	    System.out.println();
	    System.out.println();
	    System.out.println("parentCount");
	    System.out.format("%5s", "index");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", i + 1);
	    }
	    System.out.println();
	    System.out.format("%5s", " ");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", parentCount[i]);
	    }
	    System.out.println();
	    System.out.println();
	    System.out.println("jobTime");
	    System.out.format("%5s", "index");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", i + 1);
	    }
	    System.out.println();
	    System.out.format("%5s", " ");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", jobTime[i]);
	    }
	    System.out.println();
	    System.out.println();
	    System.out.println("jobDone");
	    System.out.format("%5s", "index");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", i + 1);
	    }
	    System.out.println();
	    System.out.format("%5s", " ");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", jobDone[i]);
	    }
	    System.out.println();
	    System.out.println();
	    System.out.println("jobMarked");
	    System.out.format("%5s", "index");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", i + 1);
	    }
	    System.out.println();
	    System.out.format("%5s", " ");
	    for(int i = 0; i < numNodes; i++){
	    	System.out.format("%3s", jobMarked[i]);
	    }
	    System.out.println();
	    System.out.println();
	}

	public static void outputHashTable(HashTable hashTable, PrintWriter outFile){
	    outFile.println(hashTable.getNumNodes());
	    TableNode walker;
	    for(int i = 0; i < hashTable.getNumNodes(); i++){
	        walker = hashTable.elementAt(i);
	        while(walker.getNext() != null){
	        	outFile.println(walker.getNext().getJobId() + " " + (i + 1));
	            walker = walker.getNext();
	        }
	    }
	}

	public static void outputJobTime(int[] jobTime, int numNodes, PrintWriter outFile){
		outFile.println(numNodes);
	    for(int i = 0; i < numNodes; i++){
	    	outFile.println((i + 1) + " " + jobTime[i]);
	    }
	}

	public static void outputScheduleTable(int[][] scheduleTable, int[] time, int procNeed, PrintWriter outFile){
		outFile.println("scheduleTable");
		outFile.format("%5s", "index");

	    for(int col = 0; col < time[0]; col++){
	    	String t =  "t" + (col + 1);
	    	outFile.format("%4s", t);
	    }
	    outFile.println();
	    for(int row = 0; row < procNeed; row++){
	        String p = "p" + (row + 1);
	        outFile.format("%5s", p);
	        for(int col = 0; col < time[0]; col++){
	            if(scheduleTable[row][col] == -1){
	            	outFile.format("%4s", "-");
	            }
	            else{
	            	outFile.format("%4s", scheduleTable[row][col]);
	            }
	        }
	        outFile.println();
	    }
	    outFile.println();
	}
}

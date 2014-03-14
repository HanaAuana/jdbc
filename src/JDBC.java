// Michael Lim
// JDBC Transaction Assignment

import java.sql.*;
import java.util.Random;

class JDBC {
	private static final int NUM_WORKERS = 5; //The number of worker threads we want
	static Random r = new Random();
	static boolean done = false;

	private static class WorkerThread extends Thread{
		public void run() {
			for(int i = 0; i < 10; i++){
				Connection conn = null;
				int thisPerson = r.nextInt(10)+1; //Get a random person, 1-10
				try {
					conn = DriverManager.getConnection("jdbc:mysql://database.pugetsound.edu/mlim?" //Setup JDBC connection
				                                               + "user=mlim&password=Aikahi96734");
					conn.setAutoCommit(false);
					conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

					Statement stmt=null;
					ResultSet rs=null;
					try { //Create our Statement and ResultSet
						stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,   ResultSet.CONCUR_UPDATABLE);
						rs = stmt.executeQuery("select *  from ASSIGNMENT where StudentId = "+thisPerson+"");
						System.out.println("Querying for Assignments with SId "+thisPerson);
						
						//Used this documentation for help with using the ResultSet
						//http://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html#rs_update
						while(rs.next()){ //Get the # of Tries for each Assignment, and update the count by 1
							int tries = rs.getInt("Tries");
							rs.updateInt("Tries", tries+1);
							rs.updateRow();
						}
						try { Thread.sleep(50); } //Sleep
						catch (InterruptedException e) { e.printStackTrace(); }
					}
					catch (SQLException ex){
						System.out.println("SQLException: " + ex.getMessage());
					}
					finally {
						if (rs != null) { //Make sure to close our Statement and ResultSet
							try { rs.close(); } 
							catch (SQLException sqlEx) { } // ignore
							rs = null;
						}
						if (stmt != null) {
							try { stmt.close(); } 
							catch (SQLException sqlEx) { } // ignore
							stmt = null;
						}
					}
				} catch (SQLException ex) {
					System.out.println("SQLException: " + ex.getMessage());
				} finally {
					try { //Close our connection
						if (conn != null) {
							conn.commit();
							conn.close();
						}
					} 
					catch (SQLException ex) {
						System.out.println("SQLException: " + ex.getMessage());
					}
				}
			}
		}
	}
	
	private static class CheckerThread extends Thread{
		public void run() {
			while(!done){
				Connection conn = null;

				try { //Set up our connection
					conn = DriverManager.getConnection("jdbc:mysql://database.pugetsound.edu/mlim?" + "user=mlim&password=Aikahi96734");
					conn.setAutoCommit(false);
					conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

					Statement stmt=null;
					ResultSet rs=null;
					try { //Check to see if there are any assignments with out of sync Try counts
						stmt = conn.createStatement();
						rs = stmt.executeQuery("select *  from ASSIGNMENT a1, ASSIGNMENT a2 where a1.StudentId = a2.StudentId and a1.Tries <> a2.Tries");
						
						//Used this documentation for help with using the ResultSet
						//http://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html#rs_update
						if(rs.next()){ //If all assignments don't have the same number of tries, roll back
							System.out.println("Mismatched number of Tries. Rolling back...");
							conn.rollback();
						}
						else{//Otherwise, commit
							conn.commit();
						}
					}
					catch (SQLException ex){
						System.out.println("SQLException: " + ex.getMessage());
					}
					finally {
						if (rs != null) {//Close our Statement and ResultSet
							try { rs.close(); }
							catch (SQLException sqlEx) { }
							rs = null;
						}
						if (stmt != null) {
							try { stmt.close(); } 
							catch (SQLException sqlEx) { } 
							stmt = null;
						}
					}
				} catch (SQLException ex) {
					System.out.println("SQLException: " + ex.getMessage());
				} finally {
					try {
						if (conn != null) {//Commit and close our Connection
							conn.commit();
							conn.close();
						}
					} 
					catch (SQLException ex) {
						System.out.println("SQLException: " + ex.getMessage());
					}
				}
			}
		}
	}

	public static void main(String[] args){
		Thread[] wThreads = new Thread[NUM_WORKERS];
		
		CheckerThread cThread = new CheckerThread();
		cThread.start(); //Start the Checker thread
		
		for (int i=0; i<NUM_WORKERS; i++)
		{
			wThreads[i] = new WorkerThread();//Start and start our worker threads
			wThreads[i].start(); 
		}
		
		// wait for threads to stop
		for (int i=0; i<wThreads.length; i++)
		{
			try {
				wThreads[i].join();
			} catch (InterruptedException ex) {} 		
		}
		done = true;
		try {//When workers are done, join our checker thread too
			cThread.join();
		} catch (InterruptedException e) { e.printStackTrace(); }
		System.out.println("Done");

	}

}

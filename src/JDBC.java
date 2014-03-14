import java.sql.*;
import java.util.Random;

public class JDBC {
	private static final int NUM_WORKERS = 5;
	static Random r = new Random();
	static boolean done = false;

	private static class WorkerThread extends Thread{
		public void run() {
			for(int i = 0; i < 10; i++){
				Connection conn = null;
				int thisPerson = r.nextInt(10)+1;
				try {
					conn = DriverManager.getConnection("jdbc:mysql://database.pugetsound.edu/mlim?" 
				                                               + "user=mlim&password=Aikahi96734");
					conn.setAutoCommit(false);
					conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

					Statement stmt=null;
					ResultSet rs=null;
					try {
						stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,   ResultSet.CONCUR_UPDATABLE);
						rs = stmt.executeQuery("select *  from PERSON, ASSIGNMENT where SId = "+thisPerson+" and SId = StudentId");
						System.out.println("Query successful");
						
//						while (rs.next())
//						{
//							int Tries = rs.getInt("Tries");
//							int AId = rs.getInt("AId");
//							System.out.println(Tries + " from AId "+AId);
//						}
//                      rs.first();//Rewind the result set back to the beginning
						
						//Used this documentation for help with using the ResultSet
						//http://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html#rs_update
						while(rs.next()){
							int tries = rs.getInt("Tries");
							rs.updateInt("Tries", tries+1);
							rs.updateRow();
						}
						try { Thread.sleep(50); } 
						catch (InterruptedException e) { e.printStackTrace(); }
					}
					catch (SQLException ex){
						System.out.println("SQLException: " + ex.getMessage());
					}
					finally {
						if (rs != null) {
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
					// handle any errors
					System.out.println("SQLException: " + ex.getMessage());
				} finally {
					try {
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
				int thisPerson = r.nextInt(10)+1;
				try {
					conn = DriverManager.getConnection("jdbc:mysql://database.pugetsound.edu/mlim?" + "user=mlim&password=Aikahi96734");
					conn.setAutoCommit(false);
					conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

					Statement stmt=null;
					ResultSet rs=null;
					try {
						stmt = conn.createStatement();
						rs = stmt.executeQuery("select *  from ASSIGNMENT a1, ASSIGNMENT a2 where a1.StudentId = a2.StudentId and a1.Tries <> a2.Tries");
						System.out.println("Query successful");
						
						//Used this documentation for help with using the ResultSet
						//http://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html#rs_update
						if(rs.next()){
							System.err.println("Tries don't match, exiting");
							System.exit(-1);
						}
					}
					catch (SQLException ex){
						System.out.println("SQLException: " + ex.getMessage());
					}
					finally {
						if (rs != null) {
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
					// handle any errors
					System.out.println("SQLException: " + ex.getMessage());
				} finally {
					try {
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

	public static void main(String[] args){
		Thread[] wThreads = new Thread[NUM_WORKERS+1];
		for (int i=0; i<NUM_WORKERS; i++)
		{
			wThreads[i] = new WorkerThread();
			wThreads[i].start();
		}
		
		CheckerThread cThread = new CheckerThread();
		cThread.start();

		// wait for threads to stop
		for (int i=0; i<wThreads.length; i++)
		{
			try {
				wThreads[i].join();
			} catch (InterruptedException ex) {} 		
		}
		done = true;
		try {
			cThread.join();
		} catch (InterruptedException e) { e.printStackTrace(); }

	}

}

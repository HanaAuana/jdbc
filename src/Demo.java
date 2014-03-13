import java.sql.*;

public class Demo {
	private static final int NUM_THREADS = 5;
	
	private static class TestThread extends Thread
	{
		public void run() {
			System.out.println("Thread starting");
	        Connection conn = null;
	        try {
	            conn = DriverManager.getConnection(
	                "jdbc:mysql://database.pugetsound.edu/test?" +
	                "user=guest&password=pwforguest");
	            conn.setAutoCommit(false);
	            conn.setTransactionIsolation(
	              Connection.TRANSACTION_SERIALIZABLE);

	            System.out.println("Connection successful");

	            performQuery(conn);

	            // Do something with the Connection
	        } catch (SQLException ex) {
	            // handle any errors
	            System.out.println("SQLException: " + ex.getMessage());
	        } finally {
	        	try {
	        		if (conn != null) {
	        			conn.commit();
	        			conn.close();
	        		}
	        	} catch (SQLException ex) {
	                System.out.println("SQLException: " + ex.getMessage());
	        	}
	        }
			
		}
		
	}
    public static void performQuery(Connection conn)
    {
        Statement stmt=null;
        ResultSet rs=null;
        try {
            stmt = conn.createStatement();
            rs =
            stmt.executeQuery("select * from STUDENT order by GradYear");
            System.out.println("Query successful");
            while (rs.next())
            {
                String SName = rs.getString("SName");
                int GradYear = rs.getInt("GradYear");
                System.out.println(SName + 
                    " graduated in " + GradYear);
            }

        }
        catch (SQLException ex){
            System.out.println("SQLException: " + ex.getMessage());
        }
        finally {
            try {
            	conn.commit();
            } catch (SQLException sqlEx) { } // ignore
            
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore
                stmt = null;
            }
        }

    }

    public static void main(String[] args)
    {
    	TestThread[] thread = new TestThread[NUM_THREADS];
    	for (int ii=0; ii<NUM_THREADS; ii++)
    	{
    		thread[ii] = new TestThread();
    		thread[ii].start();
    	}

    	// wait for threads to stop
    	for (int ii=0; ii<NUM_THREADS; ii++)
    	{
    		try {
    			thread[ii].join();
    		} catch (InterruptedException ex) {
    			// ignore
    		} 		
    	}

    }

}

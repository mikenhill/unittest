package com.salmon.dataload.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * BaseJDBCHelper
 * @author Ian Rogers
 *
 */
public abstract class BaseJDBCHelper {
    private static final String CLASSNAME = BaseJDBCHelper.class.getName();
    private static final Logger LOGGER = Logger.getLogger(CLASSNAME);
    private Connection jdbcConnection;
	private HashMap<String, PreparedStatement> listPS;

	/**
	 * getJdbcConnection
	 * @return Connection
	 */
	public Connection getJdbcConnection() {
        return jdbcConnection;
    }
	/**
	 * setJdbcConnection
	 * @param connection Connection
	 */
    public void setJdbcConnection(final Connection connection) {
        jdbcConnection = connection;
    }

    /**
	 * default constructor
	 */
	public BaseJDBCHelper() {
		listPS = new HashMap<String, PreparedStatement>();
	}

	/**
	 * closeConnection
	 * @throws SQLException Exception
	 */
	protected void closeConnection() throws SQLException {
		if (jdbcConnection != null && listPS != null) {
			for (Iterator<PreparedStatement> it = listPS.values().iterator(); it.hasNext();) {
				PreparedStatement ps = (PreparedStatement) it.next();
				if (ps != null) {
					ps.close();
				}
			}

			listPS.clear();
		}
		try {
		    if (jdbcConnection != null) {
		        jdbcConnection.close();
		        LOGGER.logp(Level.INFO, CLASSNAME, "BaseJDBCHelper", "Connection closed");
		    }
		} finally {
			jdbcConnection = null;
		}
	}

	/**
	 * makeConnection
	 * @param jdbcDriver String
	 * @param jdbcUrl String
	 * @param dbUser String
	 * @param dbPassword String
	 * @throws ClassNotFoundException Exception
	 * @throws SQLException Exception
	 */
	protected void makeConnection(final String jdbcDriver, final String jdbcUrl, 
	        final String dbUser, final String dbPassword) throws ClassNotFoundException, SQLException {
		if (jdbcConnection == null || jdbcConnection.isClosed()) {
			// Load the jdbc driver
			Class.forName(jdbcDriver);
		}

		// Get the connection
		jdbcConnection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
		jdbcConnection.setAutoCommit(false);
	}

	/**
	 * commitTransaction
	 * @throws SQLException Exception
	 */
	protected void commitTransaction() throws SQLException {
		if (jdbcConnection != null) {
			jdbcConnection.commit();
		}
	}

    /**
     * addPreparedStatement
     * @param stmt Statement
     * @param ps PreparedStatement
     * @throws SQLException Exception
     */
	protected void addPreparedStatement(final String stmt, final PreparedStatement ps) throws SQLException {
	    if (jdbcConnection != null) {
	        listPS.put(stmt, ps);
	    }
	}

    /**
     * getPreparedStatement
     * @param stmt String
     * @return PreparedStatement
     * @throws SQLException Exception
     */
    protected PreparedStatement getPreparedStatement(final String stmt) throws SQLException {
        PreparedStatement ps = null;
        if (jdbcConnection != null) {
            ps = listPS.get(stmt);
        }
        return ps;
    }

    /**
     * executeQuery
     * @param stmt PreparedStatement
     * @param bFlush boolean
     * @return int
     * @throws SQLException Exception
     */
	protected ResultSet executeQuery(final PreparedStatement stmt, final boolean bFlush) throws SQLException {
		return stmt.executeQuery();
	}

	/**
	 * executeUpdate
	 * @param stmt PreparedStatement
	 * @param bFlush boolean
	 * @return int
	 * @throws SQLException Exception
	 */
	protected int executeUpdate(final PreparedStatement stmt, final boolean bFlush) throws SQLException {
		return stmt.executeUpdate();
	}

}

package com.hygenics.jdbc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.hygenics.exceptions.MismatchException;
import com.hygenics.exceptions.NoClassSpecified;
import com.hygenics.exceptions.NoColumnSpecified;
import com.hygenics.exceptions.NoTableSpecifiedException;
import com.hygenics.exceptions.UnspecifiedSchemaException;
import com.hygenics.exceptions.UnspecifiedValueException;

/*THIS PROGRAM CREATES A SERIES OF DATABASE CONNECTIONS 
 * 
 * ONLY DEALS WITH STRINGS AND INTS WHICH IS WHAT THE CRAWLERS RETURN AND CAPTURE BREAKERS USE
 * TO SUPPORT MORE TYPES ADD GENERICS SUPPORT OR CREATE METHODS. UNHANDLED EXCEPTIONS ARE DEALT WITH
 * IN A CUSTOM MANNER
 * 
 * This Program is Mainly for Testing the DB while Manipulating Data but can handle larger loads.
 * Check the javadoc to see the best way to limit search results, fetch a certain number of rows; etc.
 * 
 *The program allows users to dynamically connect to a database
 * Users can either specify the username,password,and connection in the JAR
 * OR pass the password,connection url, and username to the program
 * 
 * a database and new connection can be specified at any time
 * 
 * TO START MULTIPLE CONNECTIONS SIMPLY CREATE A NEW DATABASE OBJECT
 * THEY MAY BE STORABLE
 * 
 * This program can be used to: 
 * --fetch rows
 * --fetch columns of all rows
 * --fetch a ResultSet
 * --add columns, rows, tables, and schemas
 * 
 * CONSTRUCTOR REQUIREMENTS
 * The following must be passed if using a database other than the default
 * 1. The url containing the host and database information in the format (jdbc:postgresql://host:port/database)
 * 2. The password
 * 3. the username
 */

/**
 * Full listing of JDBC functions
 * 
 * @author aevans
 *
 */
public class jdbcconn {
	private Connection conn = null;
	private Statement st = null;
	private ResultSet rs = null;
	private Statement Batch = null;

	public jdbcconn(String url, String user, String password) {
		// TODO constructor with url, user, password, and database
		// constructor for passed variables
		// contains database connection and others
		// sets the DB
		// starts the connection

		// start the connection
		Conn(url, user, password);
	}

	public jdbcconn() {
		// TODO empty constructor
		// when no variables are passed
		// the variables must be set and a new JAR created
		// probably the safest route
		Conn();
	}

	public void connectAgain() {
		// TODO reconnect to the database
		Conn();
	}

	public void connectAgain(String url, String user, String password) {
		// TODO reconnect to the database using the url, user, and password
		// provided
		Conn(url, user, password);
	}

	private void Conn() {
		// TODO empty constructor
	}

	private void delFile(File f) {
		if (f.exists()) {
			f.delete();
		}
	}

	public void CopyOut(String sql, String fpath) {
		File f = new File(fpath);
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			WritableByteChannel wChannel = raf.getChannel();
			CopyManager copyManager = new CopyManager((BaseConnection) conn);
			copyManager.copyOut(sql, Channels.newOutputStream(wChannel));
			wChannel.close();
			raf.close();

		} catch (SQLException e) {
			delFile(f);
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			delFile(f);
			e.printStackTrace();
		} catch (IOException e) {
			delFile(f);
			e.printStackTrace();
		}

	}

	private void Conn(String url, String user, String password) {
		// TODO connect to the database
		createConn(url, user, password);
	}

	public void newConn(String url, String user, String password) {
		// TODO create a new connection
		// creates a new connection
		// This does not create multiple connections
		// to create multiple connections, create a new DB object
		// the database must be set separately set

		// close any existing connection
		close();

		// create a new connection

		createConn(url, user, password);

	}

	private void createConn(String url, String user, String password) {
		// TODO create a connection
		// establishes the connection
		try {
			final Properties props = new Properties();
			props.setProperty("user", user);
			props.setProperty("password", password);
			conn = DriverManager.getConnection(url, props);

		} catch (SQLException ex) {
			System.out.println("FAILED CONNECTION!");
			ex.printStackTrace();
			close();
		}
	}

	public void close() {
		// TODO close a connection
		try {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public void doexecute(String sql) {

		try {
			st = conn.createStatement();
			st.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void truncate(String table, boolean cascade) {
		String query = "TRUNCATE " + table.trim();

		if (cascade) {
			query += " CASCADE";
		}

		query += ";";

		try {
			st = conn.createStatement();
			st.execute(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public ResultSet get_set(String inquery) {
		return execute(inquery);
	}

	private ResultSet execute(String inquery) {
		// TODO execute a statement and get a result set
		// returns a resultSet from the string passed as a parameter
		try {
			st = conn.createStatement();
			rs = st.executeQuery(inquery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	public void clearBatch() {
		try {
			Batch.clearBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void executeBatch() {
		try {
			Batch.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void addforBatch(String query) {
		try {
			if (Batch == null) {
				Batch = conn.createStatement();
			}
			Batch.addBatch(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void newBatch() {
		try {
			if (Batch == null) {

				Batch = conn.createStatement();
			} else {
				Batch.clearBatch();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addtoBatch(String query) {
		addforBatch(query);

	}

	private void execute_void_update(String inquery, String[] indata) {
		// TODO add a query with a list of strings as data
		try {
			if (inquery == null) {
				throw new UnspecifiedValueException();
			} else if (inquery.length() == 0) {
				throw new UnspecifiedValueException();
			}

			if (st != null) {
				st.close();
			}
			PreparedStatement pst = conn.prepareStatement(inquery);

			int i = 1;
			for (String d : indata) {
				pst.setString(i, d);
				i++;
			}

			pst.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		}

	}

	private void execute_void_update(String inquery) {
		// TODO executes a query
		try {
			if (inquery == null) {
				throw new UnspecifiedValueException();
			} else if (inquery.length() == 0) {
				throw new UnspecifiedValueException();
			}

			if (st != null) {
				st.close();
			}
			PreparedStatement pst = conn.prepareStatement(inquery);

			pst.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		}
	}

	private void execute_void(String inquery) {
		// TODO executes a query
		try {
			if (inquery == null) {
				throw new UnspecifiedValueException();
			} else if (inquery.length() == 0) {
				throw new UnspecifiedValueException();
			}

			if (st != null) {
				st.close();
			}
			st = conn.createStatement();

			rs = st.executeQuery(inquery);
			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		}
	}

	public String getCol(String inquery, int incolumn) {
		// TODO gets a column
		return getRow_Column(inquery, incolumn);
	}

	private String getRow_Column(String inquery, int incolumn) {
		// TODO gets a column
		// returns a column from the next row
		// get the next row if it exists from the result set
		String col_val = null;
		try {
			if (inquery == null) {
				throw new UnspecifiedValueException();
			} else if (inquery.length() == 0) {
				throw new UnspecifiedValueException();
			}

			if (rs.next()) {
				col_val = rs.getString(inquery);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		}

		return col_val;

	}

	public String[] getRow() {
		// TODO gets a row
		return rowFetcher();
	}

	public String[] getRow(String table, Boolean execute) {
		// TODO gets a row from a specific table
		// executes the query
		try {
			if (table == null) {
				throw new NoTableSpecifiedException();
			} else if (table.length() == 0) {
				throw new NoTableSpecifiedException();
			}

			if (execute == true)
				execute_again(table);
		} catch (NoTableSpecifiedException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return rowFetcher();
	}

	private void execute_again(String intable) {
		// TODO rexecutes a statement
		String query = "SELECT * FROM " + intable + ";";
		try {
			if (st != null) {
				st.close();
			}

			st = conn.createStatement();

			rs = st.executeQuery(query);

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private String[] rowFetcher() {
		/*
		 * returns a row from the result set, querying the database if a query
		 * is requested
		 */
		// returns null if there is no data
		String[] col_vals = null;

		// fetch the number of columns with a set of columns
		try {
			int num_cols = rs.getMetaData().getColumnCount();
			int i = 0;
			col_vals = new String[num_cols];

			// fetches all rows and places them in an ArrayList
			if (rs.next()) {
				for (i = 1; i < num_cols; i++) {
					col_vals[i] = rs.getString(i);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return col_vals;
	}

	public ArrayList<String[]> getRows(String inquery) {
		// TODO gets the Rows
		return fetchRows(inquery);
	}

	private ArrayList<String[]> fetchRows(String query) {
		// returns an array list of String[] containing all column values
		// the arraylist
		ArrayList<String[]> cols = new ArrayList<String[]>();

		try {
			if (query == null) {
				throw new UnspecifiedValueException();
			} else if (query.length() == 0) {
				throw new UnspecifiedValueException();
			}

			// executes the query
			execute_void(query);
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		}

		// fetch the number of columns with a set of columns
		try {
			int num_cols = rs.getMetaData().getColumnCount();
			int i = 0;
			String[] col_vals = new String[num_cols];

			// fetches all rows and places them in an ArrayList
			while (rs.next()) {
				for (i = 1; i < num_cols; i++) {
					col_vals[i] = rs.getString(i);
				}
				cols.add(col_vals);
				col_vals = new String[num_cols];
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return cols;
	}

	public void add_rows(String[] commitable, String[] cols, String intable) {
		// TODO commit multiple rows from a single string array with columns
		// separated by a |
		// make sure to separate out any pipes in the original database

		for (String row : commitable) {

			// split the data
			String[] data_cols = row.split("-:-");

			// add the row
			addRow(cols, data_cols, intable);
		}
	}

	@SuppressWarnings({ "unused" })
	public void addRow(String[] cols, String[] data, String table) {
		// TODO add a single row
		try {
			// throw critical excpetions
			if (cols.length != data.length) {
				throw new MismatchException();
			}

			if (cols == null) {
				throw new NoColumnSpecified();
			} else if (cols.length == 0) {
				throw new NoColumnSpecified();
			}

			if (data == null) {
				throw new UnspecifiedValueException();
			} else if (data.length == 0) {
				throw new UnspecifiedValueException();
			}

			if (table == null) {
				throw new NoTableSpecifiedException();
			} else if (table.length() == 0) {
				throw new NoTableSpecifiedException();
			}

			String query = "INSERT INTO " + table + " (";
			int col = 0;

			for (String c : cols) {
				if (col == 0) {
					query += c;
				} else {
					query += "," + c;
				}
				col++;
			}
			query += ") VALUES(";

			col = 0;

			for (int i = 0; i < data.length; i++) {
				if (col == 0) {
					query += "?";
				} else {
					query += "," + "?";
				}
				col++;
			}
			query += ")";

			execute_void_update(query, data);
		} catch (NoColumnSpecified e) {
			e.printStackTrace();
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		} catch (NoTableSpecifiedException e) {
			e.printStackTrace();
		} catch (MismatchException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void drop_all(String intable) {
		try {
			if (intable == null) {
				throw new UnspecifiedValueException();
			}

			String query = "DROP FROM " + intable;
			execute_void_update(query);

		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void setRow(String table, String[] cols, String[] data,
			String identifier, String id_val) {
		// TODO sets a row in the database
		try {
			if (cols == null | identifier == null) {
				throw new NoColumnSpecified();
			} else if (cols.length == 0 | identifier.length() == 0) {
				throw new NoColumnSpecified();
			}

			if (table == null) {
				throw new NoTableSpecifiedException();
			} else if (table.length() == 0) {
				throw new NoTableSpecifiedException();
			}

			if (data == null) {
				throw new UnspecifiedValueException();
			} else if (data.length == 0) {
				throw new UnspecifiedValueException();
			}

			String query = "DROP FROM " + table.trim() + " WHERE "
					+ identifier.trim() + "=" + id_val.trim();
			execute_void_update(query);

		} catch (NoColumnSpecified e) {
			e.printStackTrace();
		} catch (NoTableSpecifiedException e) {
			e.printStackTrace();
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		}
	}

	public String getColumn(int number) {
		// TODO returns a column name
		String col_name = null;
		try {
			col_name = rs.getMetaData().getColumnName(number);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return col_name;
	}

	public String[] getColumns(int[] numbers) {
		// TODO gets the column names
		String[] col_names = new String[numbers.length];
		int num = 0;

		try {
			for (int n : numbers) {
				col_names[num] = rs.getMetaData().getColumnName(n);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return col_names;
	}

	public void addColumns(String table, String[] cols) {
		// TODO adds a column
		// start the query
		String query = "ALTER TABLE " + table;
		try {
			if (cols.length == 0) {
				throw new NoColumnSpecified();
			}

			if (table == null) {
				throw new NoTableSpecifiedException();
			}

			int col_num = 0;

			for (String col : cols) {
				// create the remaining parts of the query
				query += (col_num == 0) ? " ADD COLUMN " + col.trim()
						: ", ADD COLUMN " + col.trim();
			}

			execute_void_update(query);

		} catch (NoColumnSpecified e) {
			e.printStackTrace();

		} catch (NoTableSpecifiedException e) {
			e.printStackTrace();
		}

	}

	public void setColumn(String intable, String column, String newdata) {

		// TODO sets a column name

		try {
			if (intable == null) {
				throw new NoTableSpecifiedException();
			} else if (intable.length() == 0) {
				throw new NoTableSpecifiedException();
			}

			if (column == null) {
				throw new NoColumnSpecified();
			} else if (column.length() == 0) {
				throw new NoColumnSpecified();
			}

			if (newdata == null) {
				throw new UnspecifiedValueException();
			}

			// create and execute the query
			String query = "Alter Table " + intable + " SET " + column.trim()
					+ "=" + newdata.trim();
			execute_void_update(query);
		} catch (NoTableSpecifiedException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (NoColumnSpecified e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public String[] getTables(String schema) {
		// TODO gets the column names
		String[] tables = null;
		int col = 0;
		try {
			if (schema == null) {
				throw new UnspecifiedSchemaException();
			} else if (schema.length() == 0) {
				throw new UnspecifiedSchemaException();
			}
			// build the query
			String query = "SELECT table_name FROM information_schema.tables WHERE table_schema='"
					+ schema.trim() + "' ORDER BY table_name";
			execute_void(query);

			if (rs.last()) {
				tables = new String[rs.getRow()];
				rs.beforeFirst();
			}

			while (rs.next()) {
				tables[col] = rs.getString("table_name");
			}

		} catch (UnspecifiedSchemaException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return tables;
	}

	public void addTable(String intable, String[] cols, String[] types) {
		// TODO creates a table
		// start the query
		String query = "CREATE TABLE " + intable.trim() + " (";
		try {
			if (cols.length == 0) {
				throw new NoColumnSpecified();
			}

			if (types.length == 0) {
				throw new NoClassSpecified();
			}

			int col_num = 0;

			// finisht the query after checking for exceptions
			for (String col : cols) {
				query += (col_num == 0) ? col : "," + col;
				col_num++;
			}

			query += ")";

			execute_void_update(query);
		} catch (NoColumnSpecified e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (NoClassSpecified e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}

	public void createSchema(String inschema) {
		// TODO create a schema
		try {
			if (inschema == null) {
				throw new UnspecifiedSchemaException();
			} else if (inschema.length() == 0) {
				throw new UnspecifiedSchemaException();
			}

			// the query
			String query = "CREATE SCHEMA " + inschema;
			execute_void(query);
		} catch (UnspecifiedSchemaException e) {
			e.printStackTrace();
		}
	}

	public void dropSchema(String schema) {
		// TODO drops a schema

		try {
			if (schema == null) {
				throw new UnspecifiedValueException();
			} else if (schema.length() == 0) {
				throw new UnspecifiedValueException();
			}

			// the query
			String query = "DROP SCHEMA " + schema.trim();
			execute_void(query);
		} catch (UnspecifiedValueException e) {
			e.printStackTrace();
		}
	}

}

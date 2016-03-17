package com.hygenics.parser;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;

import mjson.Json;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.scheduling.annotation.Async;

import com.eclipsesource.json.JsonObject;

/**
 * The Data Source for the Page Grab
 * 
 * 
 * @author aevans
 *
 */
public class getDAOTemplate {

	private static final Logger log = LoggerFactory.getLogger(MainApp.class);
	

	@Autowired
	@Qualifier("jdbcTemplate")
	private JdbcTemplate jdbcTemplateObject;
	
	private int uniqueid = 0;

	public getDAOTemplate() {

	}

	

	/**
	 * Checks for a table schema
	 * 
	 * @param schema
	 * @return
	 */
	public boolean checkSchema(String schema) {
		String sql = "SELECT count(schema_name) FROM information_schema.schemata WHERE schema_name='"
				+ schema + "'";

		SqlRowSet rs = this.jdbcTemplateObject.queryForRowSet(sql);

		if (rs.next()) {
			if (rs.getInt(1) > 0) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Check Table
	 */

	public boolean checkTable(String table, String schema) {
		String[] table_split = table.split("\\.");

		if (table_split.length > 0) {

			String sql = "SELECT count(table_name) FROM information_schema.tables WHERE table_name='"
					+ table_split[1] + "' AND table_schema='" + schema + "'";
			log.info(sql);
			SqlRowSet rs = this.jdbcTemplateObject.queryForRowSet(sql);

			if (rs.next()) {
				if (rs.getInt(1) > 0) {
					return true;
				}
			}

		}
		return false;
	}

	/**
	 * Called when needing a count of distincts or other numerical result.
	 * Returns an Integer so only an integer should be used
	 * 
	 * @param sql
	 * @param columns
	 * @return
	 */
	public int queryForInt(String sql) {
		SqlRowSet rs = this.jdbcTemplateObject.queryForRowSet(sql);

		if (rs.next()) {
			if (rs.getMetaData().getColumnNames().length > 0) {
				return rs.getInt(1);
			}
		}
		return 0;

	}

	/**
	 * Called when needing a count of distincts or other numerical result.
	 * Returns an Big Integer so only an integer should be used
	 * 
	 * @param sql
	 * @param columns
	 * @return
	 */
	public BigDecimal queryForBigInt(String sql) {
		SqlRowSet rs = this.jdbcTemplateObject.queryForRowSet(sql);

		if (rs.next()) {
			if (rs.getMetaData().getColumnNames().length > 0) {
				return rs.getBigDecimal(1);
			}
		}
		return new BigDecimal(0);
	}

	/**
	 * Called when needing a count of distincts or other numerical result.
	 * Returns a Float so only an integer should be used
	 * 
	 * @param sql
	 * @param columns
	 * @return
	 */
	public float queryForFloat(String sql) {
		SqlRowSet rs = this.jdbcTemplateObject.queryForRowSet(sql);

		if (rs.next()) {
			if (rs.getMetaData().getColumnNames().length > 0) {

				return rs.getFloat(1);
			}
		}
		return 0f;

	}

	/**
	 * Called when needing a count of distincts or other numerical result.
	 * Returns an Integer so only an integer should be used
	 * 
	 * @param sql
	 * @param columns
	 * @return
	 */
	public double queryForDoubleString(String sql) {
		SqlRowSet rs = this.jdbcTemplateObject.queryForRowSet(sql);

		if (rs.next()) {
			if (rs.getMetaData().getColumnNames().length > 0) {
				return rs.getDouble(1);
			}
		}
		return 0;

	}

	/**
	 * Create a Schema
	 */
	public void createSchema(String schema) {
		String sql = "CREATE SCHEMA IF NOT EXISTS " + schema;
		this.jdbcTemplateObject.execute(sql);
	}

	/**
	 * Create a Table with a map
	 */
	public void createTablebyMap(String table, String schema,
			Map<String, String> columns, String key, String foreignkey,
			String fkeyref) {
		String fkey = foreignkey;
		String pkey = key;
		String sql;
		Set<String> keys = columns.keySet();
		if (checkTable(table, schema) == false & columns.size() > 0) {
			if (key != null) {

				if (fkey == null) {
					fkey = "";
				}

				sql = "CREATE TABLE IF NOT EXISTS " + table
						+ " (id SERIAL UNIQUE NOT NULL";
			} else {
				pkey = "";
				fkey = "";
				sql = "CREATE TABLE IF NOT EXISTS " + table
						+ " (id SERIAL PRIMARY KEY NOT NULL";
			}

			for (String c : keys) {
				if (c.compareTo("table") != 0) {
					if (c.compareTo(pkey) != 0) {
						sql += "," + columns.get(c) + " text";
					} else if (c.compareTo(fkey) == 0) {
						sql += ","
								+ columns.get(c).replaceAll(
										"(?mis)soup.*|psinglep", "")
								+ " text FOREIGN KEY REFERENCES " + fkeyref;
					} else {
						sql += ","
								+ columns.get(c).replaceAll(
										"(?mis)soup.*|psinglep", "")
								+ " text PRIMARY KEY";
					}
				}
			}

			sql += ",date text, offenderhash text)";
			this.jdbcTemplateObject.execute(sql);
		}
	}

	/**
	 * Create a Table with a set
	 */
	public void createTable(String table, String schema, Set<String> columns,
			String inpkey, String infkey, String fkeyref) {
		String fkey = infkey;
		String pkey = inpkey;
		String sql = null;
		;

		if (checkTable(table, schema) == false & columns.size() > 0) {
			if (pkey != null) {
				sql = "CREATE TABLE IF NOT EXISTS " + table
						+ "(id SERIAL UNIQUE NOT NULL";
			} else {
				pkey = "";
				fkey = "";
				sql = "CREATE TABLE IF NOT EXISTS  " + table
						+ "(id SERIAL PRIMARY KEY NOT NULL";
			}

			for (String c : columns) {
				if (c.compareTo("table") != 0) {
					if (c.compareTo(fkey) == 0) {
						sql += "," + c.replaceAll("(?mis)soup.*|psinglep", "")
								+ " text FOREIGN KEY REFERENCES " + fkeyref;
					} else if (c.compareTo(pkey) == 0) {
						sql += "," + c.replaceAll("(?mis)soup.*|psinglep", "")
								+ " text PRIMARY KEY";
					} else {
						sql += "," + c.replaceAll("(?mis)soup.*|psinglep", "")
								+ " text";
					}
				}
			}

			if (pkey.compareTo("offenderhash") == 0) {
				sql += ",date text, offenderhash text PRIMARY KEY)";
			} else if (fkey.compareTo("offenderhash") == 0) {
				sql += ",date text, offenderhash text FOREIGN KEY REFERENCES "
						+ fkeyref + ")";
			} else {
				sql += ",date text, offenderhash text)";
			}

			log.info(sql);
			this.jdbcTemplateObject.execute(sql);
		}
	}

	/**
	 * Create a Column
	 * 
	 */
	public void createColumn(String table, String column) {
		String sql = "ALTER TABLE " + table + " ADD COLUMN " + column + " text";
		this.jdbcTemplateObject.execute(sql);
	}

	/**
	 * check for column
	 */
	public boolean columnExists(String table, String column) {
		String[] data = table.split("\\.");
		if (data.length > 1) {
			String query = "SELECT count(column_name) FROM information_schema.columns WHERE table_name='"
					.trim()
					+ data[1].trim()
					+ "' AND table_schema='"
					+ data[0]
					+ "' AND column_name='" + column.trim() + "'";
			SqlRowSet rs = this.jdbcTemplateObject.queryForRowSet(query);

			if (rs.next()) {
				if (rs.getInt(1) != 0) {
					return true;
				}
			}

		}

		return false;
	}

	/**
	 * Execute an non-returning query
	 * 
	 * @param query
	 */
	public void execute(String query) {
		this.jdbcTemplateObject.execute(query);
	}

	/**
	 * Batch Update Script Using a Single Variable
	 * 
	 * @param table
	 * @param values
	 * @param column
	 * @param commit_size
	 */
	public void batchUpdateSingle(String table, ArrayList<String> values,
			String column, int commit_size) {
		String query = "INSERT INTO " + table + "(" + column + ") VALUES(?)";
		this.jdbcTemplateObject.batchUpdate(query,
				getBatchSingle(values, commit_size));
	}

	/**
	 * Get Single Batch
	 * 
	 * @param values
	 * @param commit_size
	 * @return
	 */
	private BatchPreparedStatementSetter getBatchSingle(
			final ArrayList<String> values, final int commit_size) {
		return (new BatchPreparedStatementSetter() {

			@Override
			public int getBatchSize() {
				// TODO Auto-generated method stub
				return values.size();
			}

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				// TODO Auto-generated method stub

				ps.setString(1, values.get(i));

			}

		});

	}

	/**
	 * Return the column names
	 * 
	 * @return
	 */
	public ArrayList<String> getColumns(String table) {
		String[] split = table.split("\\.");
		String sql = null;

		if (split.length == 2) {
			sql = "SELECT column_name FROM information_schema.columns WHERE table_name='"
					+ split[1].trim()
					+ "' AND table_schema='"
					+ split[0].trim() + "'";
		} else {
			// get rid of obscure sql error
			try {
				throw new NullPointerException(
						"Table must be provided in schema.table format!");
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		}

		return this.jdbcTemplateObject.query(sql,
				getArrayResultSetExtractor("column_name"));
	}

	/**
	 * Takes a sql statement and returns the given select columns.
	 * 
	 * @param select
	 *            -String sql statement
	 * @return
	 */
	public ArrayList<String> getAll(String select) {

		return this.jdbcTemplateObject.query(select, getAllExtractor());
	}

	private ResultSetExtractor<ArrayList<String>> getAllExtractor() {
		// create the array list of json strings for the query
		return (new ResultSetExtractor<ArrayList<String>>() {

			@Override
			public ArrayList<String> extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				ArrayList<String> results = new ArrayList<String>(rs.getFetchSize());
				while (rs.next()) {
					JsonObject jobj = new JsonObject();
					for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
						jobj.add(rs.getMetaData().getColumnName(i),rs.getString(i));
					}
					results.add(jobj.toString());
				}

				return results;
			}

		});
	}

	/**
	 * Gets Data in Map<String,String> format based on a table and two columns
	 * 
	 * @param select
	 * @param column
	 * @param idcolumn
	 * @return
	 */
	public Map<String, String> getData(String select, String column,
			String idcolumn, boolean unique) {
		return this.jdbcTemplateObject.query(select,
				getExtractor(column, idcolumn, unique));
	}

	public ResultSetExtractor<Map<String, String>> getExtractor(
			final String column, final String idcolumn, final boolean unique) {
		return (new ResultSetExtractor<Map<String, String>>() {

			@Override
			public Map<String, String> extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				// TODO Auto-generated method stub
				Map<String, String> results = new HashMap<String, String>();

				while (rs.next()) {
					if (unique == false) {
						results.put(rs.getString(column), rs
								.getString(idcolumn).replaceAll("\"", ""));
					} else {
						String id = rs.getString(column);
						id += "UQUQ" + uniqueid + "UQUQ";
						results.put(id, rs.getString(idcolumn));
						uniqueid++;
					}
				}
				rs.close();
				return results;
			}

		});
	}

	/**
	 * Gets an arraylist of strings corresponding to the column passe in
	 * 
	 * @param sql
	 * @param column
	 * @return
	 */
	public ArrayList<String> getArrayList(String sql, String column) {
		return this.jdbcTemplateObject.query(sql,
				getArrayResultSetExtractor(column));
	}

	/**
	 * Extractor for ArrayList
	 * 
	 * @param column
	 * @return
	 */
	private ResultSetExtractor<ArrayList<String>> getArrayResultSetExtractor(
			final String column) {
		return (new ResultSetExtractor<ArrayList<String>>() {

			@Override
			public ArrayList<String> extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				// TODO Auto-generated method stub
				ArrayList<String> results = new ArrayList<String>();

				while (rs.next()) {
					results.add(rs.getString(column));
				}

				rs.close();
				return results;
			}

		});
	}

	/**
	 * The Original getJsonData replaced quotes. This does not.
	 * 
	 * @param query
	 * @return
	 */
	public ArrayList<String> getJSonDatawithQuotes(String query) {
		return this.jdbcTemplateObject.query(query,getJsonwithQuotesExtractor());
	}

	public ResultSetExtractor<ArrayList<String>> getJsonwithQuotesExtractor() {
		return (new ResultSetExtractor<ArrayList<String>>() {

			@Override
			public ArrayList<String> extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				// TODO Auto-generated method stub
				int j = 0;
				ArrayList<String> results = new ArrayList<String>();
				JsonObject obj = null;
				int columns = 0;

				while (rs.next()) {
					if (columns == 0) {
						columns = rs.getMetaData().getColumnCount();
						if (columns == 0) {
							return results;
						}
					}

					obj = null;
					obj = new JsonObject();
					for (int i = 1; i <= columns; i++) {
						obj.add(rs.getMetaData().getColumnName(i),StringEscapeUtils.escapeJson(rs.getString(i).replaceAll("\t|\r|\n|\r\n|", "")));
					}
					if (obj != null) {
						results.add(obj.toString());
					}

					j++;
				}

				if (rs.getFetchSize() <= j) {
					rs.close();
				}

				return results;
			}

		});
	}

	/**
	 * Returns a string in JSon notation of the data. This saves from needing a
	 * map of maps.
	 * 
	 * @return
	 */
	public ArrayList<String> getJsonData(String query) {
		return this.jdbcTemplateObject.query(query, getJsonExtractor());
	}

	/**
	 * Returns a Json Based Extractor
	 * 
	 * @return
	 */
	public ResultSetExtractor<ArrayList<String>> getJsonExtractor() {
		return (new ResultSetExtractor<ArrayList<String>>() {

			@Override
			public ArrayList<String> extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				// TODO Auto-generated method stub
				int j = 0;
				ArrayList<String> results = new ArrayList<String>();
				JsonObject obj = null;
				int columns = 0;

				while (rs.next()) {
					if (columns == 0) {
						columns = rs.getMetaData().getColumnCount();
						if (columns == 0) {
							return results;
						}
					}

					obj = null;
					obj = new JsonObject();
					for (int i = 1; i <= columns; i++) {
						if (rs.getString(i) != null) {
							obj.add(rs.getMetaData().getColumnName(i),
									rs.getString(i).replaceAll(
											"\t|\r|\n|\r\n|", ""));
						} else {
							obj.add(rs.getMetaData().getColumnName(i), "");
						}
					}
					if (obj != null) {
						results.add(obj.toString());
					}

					j++;
				}

				if (rs.getFetchSize() <= j) {
					rs.close();
				}

				return results;
			}

		});
	}

	/**
	 * Since the json reader in scala forgets key order, allows
	 * for the posting of data with a preset keylist,sql, and data
	 */
	public void postJsonDataWithOrder(String sql,ArrayList<String> jsondata, ArrayList<String> keyOrder){
		if (jsondata.size() > 0) {
			// post the keys to the database
			this.jdbcTemplateObject.batchUpdate(sql,jsonBatchSetter(jsondata, keyOrder));
		}
	}
	
	
	/**
	 * Controls the posting of variable minimal json data
	 */
	public void postJsonData(String sql, ArrayList<String> jsondata) {
		// get the keyset order of the first posted string
		if (jsondata.size() > 0) {
			ArrayList<String> keys = new ArrayList<String>();
			Map<String, Json> jmap = Json.read(jsondata.get(0)).asJsonMap();

			for (String k : jmap.keySet()) {
				if (k.toLowerCase().compareTo("table") != 0 && !k.toLowerCase().contains("narrow")) {
					keys.add(k);
				}
			}

			// post the keys to the database
			this.jdbcTemplateObject.batchUpdate(sql,jsonBatchSetter(jsondata, keys));
			keys = null;
			jmap = null;
		}
	}

	private BatchPreparedStatementSetter jsonBatchSetter(
			final ArrayList<String> jsondata, final ArrayList<String> keys) {
		return (new BatchPreparedStatementSetter() {

			@Override
			public int getBatchSize() {
				return jsondata.size();
			}

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				Map<String, Json> jmap = Json.read(jsondata.get(i)).asJsonMap();
				int j = 0;

				for (String k : keys) {
					if (k.toLowerCase().compareTo("table") != 0 && !k.toLowerCase().contains("narrow")) {
						j++;
						ps.setString(j, StringEscapeUtils.unescapeJson(jmap.get(k).asString()));
					}
				}
			}

		});

	}

	/**
	 * posts a single json row
	 * 
	 * @param json
	 */
	public void postSingleJson(String json) {

		try {
			Map<String, Json> jmap = Json.read(json).asJsonMap();

			String sql = "INSERT INTO " + jmap.get("table").asString() + "(";
			String val = " VALUES(";
			int i = 0;

			for (String k : jmap.keySet()) {
				if (k.trim().compareTo("table") != 0) {
					sql = (i == 0) ? sql + k : sql + "," + k;
					val = (i == 0) ? val + "'"+ jmap.get(k).asString().trim().replaceAll("'", "")+ "'" : val + ",'"+ jmap.get(k).asString().trim().replaceAll("'", "")+ "'";
					i++;
				}
			}

			sql += ") " + val + ")";

			this.jdbcTemplateObject.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Posts Json Data with table in the Json String.
	 */
	@Async
	public void postJsonDatawithTable(ArrayList<String> json) {
		ArrayList<Map<String,Json>> sublist = new ArrayList<Map<String,Json>>();
		ArrayList<String> keylist = new ArrayList<String>();
		String table = "placeholder";
		String sql = null;
		Set<String> keys;
		int numkeys = 0;
		int j = 0;
		Map<String, Json> jmap = null;

		for (int i = 0; i < json.size(); i++) {
			String js = json.get(i).replaceAll("\"\"", "\"");
			jmap = Json.read(js).asJsonMap();

			if (jmap.containsKey("table")) {
				if (jmap.get("table").asString().compareTo(table) == 0 && jmap.keySet().size() == numkeys && sublist.size() <= 100) {
					jmap.remove("table");
					sublist.add(jmap);
				} else {
					if (sql != null) {
						this.jdbcTemplateObject.batchUpdate(sql,getJsonwithTableSetter(sublist, keylist));
						sublist = new ArrayList<Map<String,Json>>();
					}

					keys = jmap.keySet();
					sql = "INSERT INTO " + jmap.get("table").asString() + "(";

					j = 0;
					numkeys = keys.size();
					keylist = new ArrayList<String>();

					for (String k : keys) {

						if (k.compareTo("table") == 0) {
							table = jmap.get(k).asString().trim();
						} else {
							keylist.add(k.trim());
							sql = (j == 0) ? sql + k.trim() : sql + ","
									+ k.trim();
						}
						j++;
					}

					sql += ") VALUES(";
					j = 0;

					for (String k : keys) {
						if (k.compareTo("table") != 0) {
							sql = (j == 0) ? sql + "?" : sql + ",?";
							j++;
						}
					}
					sql += ");";
					jmap.remove("table");
					sublist.add(jmap);
				}
			}
		}

		log.info(""+sublist.size());
		
		if (sublist.size() > 0) {
			this.jdbcTemplateObject.batchUpdate(sql,getJsonwithTableSetter(sublist, keylist));
		}

	}

	private BatchPreparedStatementSetter getJsonwithTableSetter(
			final ArrayList<Map<String,Json>> json, final ArrayList<String> keys) {
		return (new BatchPreparedStatementSetter() {

			@Override
			public int getBatchSize() {
				return json.size();
			}

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				Map<String, Json> jmap = json.get(i);
				int k = 1;

				for (String key : keys) {
					if (key.compareTo("table") != 0) {
						ps.setString(k, jmap.get(key).asString().trim());
						k++;
					}
				}

			}

		});
	}

	/**
	 * Returns a column
	 * 
	 * @param data
	 *            -column information
	 */
	public String getColumn(String sql, String column) {
		return this.jdbcTemplateObject.query(sql, getColumnExtractor(column));
	}

	/**
	 * Column Extractor
	 * 
	 * @return
	 */
	public ResultSetExtractor<String> getColumnExtractor(final String column) {
		return (new ResultSetExtractor<String>() {

			@Override
			public String extractData(ResultSet rs) throws SQLException,
					DataAccessException {

				String result = null;

				if (rs.next()) {
					result = rs.getString((column.trim()));
				}
				rs.close();

				return result;
			}
		});
	}

	/**
	 * Get a count from a table
	 * 
	 * @param table
	 * @return
	 */
	public int getCount(String table) {
		String sql = "Select count(*) FROM " + table;
		return this.jdbcTemplateObject.queryForObject(sql, Integer.class);
	}

}

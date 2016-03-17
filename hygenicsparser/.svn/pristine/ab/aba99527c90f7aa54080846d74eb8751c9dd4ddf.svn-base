package com.hygenics.parser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;


import com.eclipsesource.json.Json;
import com.hygenics.exceptions.MissingData;
import com.hygenics.exceptions.NoDataException;
import com.hygenics.exceptions.SQLMalformedException;
import com.hygenics.exceptions.TableMissingException;
import com.hygenics.jdbc.jdbcconn;

/**
 * Takes in a series of attributes and dumps them based on their specifications.
 * This is useful for maintaining common schemas.
 * 
 * Input is a Map<table<Map<attribute type/table name, attr/table name>>
 * 
 * Types of keys are the name of the table as a string combined with the
 * filename as table | filename attr - common attr as String
 * 
 * Specifications for attributes distinct-for concacting distinct part of query
 * not0-for specifiying that the length must be greater than 0 in the WHERE
 * clause group-for grouping the attribute not null-for specifying that the attr
 * cannot be null
 * 
 * @author asevans
 *
 */
public class SpecDumpWithReference {
	private final static String datestamp= Calendar.getInstance().getTime().toString().trim().replaceAll(":|\\s", "");
	
	
	private Logger log = LoggerFactory.getLogger(MainApp.class);

	private ArrayList<String> tablesMustHave = null;

	private String baseschema;
	private String baseFile;

	private String delimiter = "|";
	private String replacedel = ";";
	private String url;
	private String user;
	private String pass;
	private boolean unicoderemove = true;
	private Map<String, Map<String, String>> tables;
	

	@Autowired
	private getDAOTemplate template;
	private String extracondition;

	private boolean archive = true;


	public String getReplacedel() {
		return replacedel;
	}

	public void setReplacedel(String replacedel) {
		this.replacedel = replacedel;
	}

	public void setBaseschema(String baseschema) {
		this.baseschema = baseschema;
	}
	
	public ArrayList<String> getTablesMustHave() {
		return tablesMustHave;
	}

	public void setTablesMustHave(ArrayList<String> tablesMustHave) {
		this.tablesMustHave = tablesMustHave;
	}

	public boolean isArchive() {
		return archive;
	}

	public void setArchive(boolean archive) {
		this.archive = archive;
	}

	public SpecDumpWithReference() {

	}

	public String getBaseschema() {
		return baseschema;
	}

	

	public String getBaseFile() {
		return baseFile;
	}

	@Required
	public void setBaseFile(String baseFile) {
		this.baseFile = baseFile;
	}

	public String getreplacedel() {
		return replacedel;
	}

	public void setreplacedel(String replacedel) {
		this.replacedel = replacedel;
	}

	public boolean isUnicoderemove() {
		return unicoderemove;
	}

	public void setUnicoderemove(boolean unicoderemove) {
		this.unicoderemove = unicoderemove;
	}

	public String getExtracondition() {
		return extracondition;
	}

	public void setExtracondition(String extracondition) {
		this.extracondition = extracondition;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public Map<String, Map<String, String>> getTables() {
		return tables;
	}

	/**
	 * Set the tables and attributes
	 * 
	 * @param tables
	 */
	public void setTables(Map<String, Map<String, String>> tables) {
		this.tables = tables;
	}

	protected class ToFile extends RecursiveAction {
		private final String sql;
		private final String file;

		public ToFile(final String sql, final String file) {
			this.sql = sql;
			this.file = file;
		}

		@Override
		public void compute() {
			jdbcconn conn = new jdbcconn(url, user, pass);
			conn.CopyOut(sql.trim(), (file.trim() + datestamp + ".txt").trim());
		}
	}

	/**
	 * Runs the Dump
	 */
	public void run() {

		if (archive) {
			Archiver zip = new Archiver();
			String[] barr = baseFile.split("\\/");
			String basefile = "";
			for (int i = 0; i > barr.length - 1; i++) {
				basefile += (i == 0) ? barr[i] : "/" + barr[i];
			}
			if (basefile.trim().length() > 0) {
				zip.setBasedirectory(basefile);
				zip.setZipDirectory(basefile + "archive.zip");
				zip.setAvoidanceString(".zip|archive");
				zip.setDelFiles(true);
				zip.run();
			}
		}

		int dumped = 0;
		log.info("Tables Found: " + tables.size());
		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors());
		boolean checkedTables = (this.tablesMustHave == null);
		for (String tf : tables.keySet()) {
			String[] split = (this.baseschema + "." + tf + "|" + this.baseFile + tf)
					.split("\\|");
			log.info("Dumping for " + split[0]);
			String schema = null;
			try {
				schema = split[0].split("\\.")[0];

				if (!checkedTables) {
					ArrayList<String> mustHaveTemp = (ArrayList<String>) this.tablesMustHave.clone();
					ArrayList<String> existingTables = this.template.getJsonData("SELECT table_name FROM information_schema.tables WHERE table_schema ILIKE '%"+ schema + "%'");
					for (String tdict : existingTables) {
					
						String table = Json.parse(tdict).asObject().get("table_name").asString();
						if (mustHaveTemp.contains(table)) {
							mustHaveTemp.remove(table);

							// get count
							if (this.template.getCount(schema + "." + table) == 0) {
								try {
									throw new MissingData("Data Missing from Required Table: "+ schema + "." + table);
								} catch (MissingData e) {
									e.printStackTrace();
									if(tablesMustHave.contains(table)){
										log.error("Critical Table Missing Data! Terminating!");
										System.exit(-1);
									}
								}
							}

						}
					}

					if (mustHaveTemp.size() > 0) {
						log.error("Drop Schema " + schema+ "  is missing the following tables:\n");
						for (String table : mustHaveTemp){
							log.error(table + "\n");
						}

						try {
							throw new TableMissingException();
						} catch (TableMissingException e) {
							e.printStackTrace();
							System.exit(-1);
						}
					}
				}

			} catch (IndexOutOfBoundsException e) {
				try {
					throw new SQLMalformedException("FATAL ERROR: Table name "+ split[0] + " malformed");
				} catch (SQLMalformedException e2){
					e2.printStackTrace();
					System.exit(-1);
				}
			}

			log.info("Checking  table: " + split[0] + "&& schema: " + schema);

			if (template.checkTable(split[0], schema)) {
				// check if there are records

				if (template.getCount(schema + "."+ split[0].replace(schema + ".", "")) > 0) {
					dumped += 1;
					Set<String> keys = tables.get(tf).keySet();
					String sql;
					String select = "SELECT ";
					String distinct = null;
					String attrs = null;
					String where = null;
					String group = null;
					String order = null;

					/**
					 * SET THE ATTRIBUTES WHICH CAN BE SPECIFIED WITH
					 * distinct-for concacting distinct part of query not0-for
					 * specifiying that the length must be greater than 0 in the
					 * WHERE clause group-for grouping the attribute not
					 * null-for specifying that the attr cannot be null
					 * orderby-for specifying our one order attr
					 */
					for (String k : keys) {
						if (k.toLowerCase().contains("distinct")) {
							distinct = (distinct == null) ? "distinct on("
									+ tables.get(tf).get(k)
											.replaceAll("\\sas.*", "")
									: distinct
											+ ","
											+ tables.get(tf).get(k)
													.replaceAll("\\sas.*", "");
						}

						if (k.toLowerCase().contains("group")) {
							group = (group == null) ? "GROUP BY "
									+ tables.get(tf).get(k)
											.replaceAll("\\sas.*", "") : group
									+ ","
									+ tables.get(tf).get(k)
											.replaceAll("\\sas.*", "");
						}

						if (k.toLowerCase().contains("not0")) {
							if (k.contains("not0OR")) {
								where = (where == null) ? "WHERE length("
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ ") >0 " : where
										+ "OR length("
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ ")";
							} else {
								where = (where == null) ? "WHERE length("
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ ") >0 " : where
										+ "AND length("
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ ")";
							}
						}

						if (k.toLowerCase().contains("notnull")) {
							if (k.toLowerCase().contains("notnullor")) {
								where = (where == null) ? "WHERE "
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ " IS NOT NULL" : where
										+ " OR "
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ " IS NOT NULL";
							} else {
								where = (where == null) ? "WHERE "
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ " IS NOT NULL" : where
										+ " AND "
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ " IS NOT NULL";
							}
						}

						if (k.toLowerCase().contains("order")) {
							if (k.toLowerCase().contains("orderdesc")) {
								order = (order == null) ? "ORDER BY "
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ " ASC" : order;
							} else {
								order = (order == null) ? "ORDER BY "
										+ tables.get(tf).get(k)
												.replaceAll("\\sas.*", "")
										+ " DESC" : order;
							}
						}

						String field = tables.get(tf).get(k);
						if (k.toLowerCase().contains("attr")) {
							if (unicoderemove == true) {
								field = "regexp_replace(trim(replace(regexp_replace(cast("
										+ field+" as text)"
										+ ",'[^\\u0020-\\u007e,\\(\\);\\-\\[\\]]+',' '),'"
										+ this.delimiter + "','"
										+ this.replacedel + "')),'[\\r|\\n]+','	','gm') as " + field;
							} else {
								field = "regexp_replace(trim(replace(cast(" + field + " as text),'"
										+ this.delimiter + "','"
										+ this.replacedel + "')),'[\\r|\\n]+','	','gm')";
							}

							attrs = (attrs == null) ? field : attrs + ","
									+ field;
						}
					}

					select = (distinct == null) ? select : select.trim() + " "
							+ distinct.trim() + ")";
					select += " " + attrs.trim();
					select += " FROM " + split[0].trim();
					select = (where == null) ? select : select.trim() + " "
							+ where.trim();
					select = (group == null) ? select : select.trim() + " "
							+ group.trim();
					select = (order == null) ? select : select.trim() + " "
							+ order.trim();

					if (extracondition != null) {
						select += (select.contains(" WHERE ") == true) ? " AND"
								+ extracondition : " WHERE " + extracondition;
					}

					select = select.trim();

					log.info("Dump Select Command: " + select);
					
					sql = "COPY  (" + select + ") TO STDOUT WITH DELIMITER '"+ delimiter.trim() + "' NULL as '' CSV HEADER";
					fjp.execute(new ToFile(sql, split[1].trim()));

					select = "SELECT ";
					distinct = null;
					attrs = null;
					where = null;
					group = null;
					order = null;
				} else {
					try {
						
						throw new NoDataException("WARNING: Table " + split[0]+ " has no Data");
				
					} catch (NoDataException e) {
						e.printStackTrace();
						if(tablesMustHave != null && tablesMustHave.contains(split[0])){
							log.error("Table is a Must Have Table by has not Data. Terminating!");
							System.exit(-1);
						}
					}
				}
			} else {
				try {
					throw new SQLMalformedException("WARNING: Table "+ split[0] + " is missing");
				} catch (SQLMalformedException e) {
					e.printStackTrace();
				}
			}
		}

		try {
			fjp.awaitTermination(60000, TimeUnit.MILLISECONDS);
			fjp.shutdown();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (dumped == 0) {
			log.info("No Data Found in any Table");
			System.exit(-1);
		}

	}

}

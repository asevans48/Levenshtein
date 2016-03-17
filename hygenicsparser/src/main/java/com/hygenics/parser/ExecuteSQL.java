package com.hygenics.parser;

import java.text.DateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.eclipsesource.json.JsonObject;

public class ExecuteSQL {
	
	
	private Map<String, String> replaces;
	

	private String idval;
	
	
	private String schemaColumn;
	
	
	private String tableSchema;
	
	
	private String tableColumn;
	
	
	private String tableIdentifier;
	
	
	private Map<String, String> tableMappings;
	
	private Logger log = LoggerFactory.getLogger(MainApp.class);
	
	
	private List<String> sql;
	
	@Autowired
	private getDAOTemplate template;
	
	
	private boolean kill=false;
	
	
	private Map<String, List<String>> move;

	
	public Map<String, String> getReplaces() {
		return replaces;
	}

	public void setReplaces(Map<String, String> replaces) {
		this.replaces = replaces;
	}

	public String getIdval() {
		return idval;
	}

	public void setIdval(String idval) {
		this.idval = idval;
	}

	public String getSchemaColumn() {
		return schemaColumn;
	}

	public void setSchemaColumn(String schemaColumn) {
		this.schemaColumn = schemaColumn;
	}

	public String getTableSchema() {
		return tableSchema;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

	public String getTableColumn() {
		return tableColumn;
	}

	public void setTableColumn(String tableColumn) {
		this.tableColumn = tableColumn;
	}

	public String getTableIdentifier() {
		return tableIdentifier;
	}

	public void setTableIdentifier(String tableIdentifier) {
		this.tableIdentifier = tableIdentifier;
	}

	public Map<String, String> getTableMappings() {
		return tableMappings;
	}

	public void setTableMappings(Map<String, String> tableMappings) {
		this.tableMappings = tableMappings;
	}

	public Logger getLog() {
		return log;
	}

	public void setLog(Logger log) {
		this.log = log;
	}

	public List<String> getSql() {
		return sql;
	}

	public void setSql(List<String> sql) {
		this.sql = sql;
	}

	public boolean isKill() {
		return kill;
	}

	public void setKill(boolean kill) {
		this.kill = kill;
	}

	public Map<String, List<String>> getMove() {
		return move;
	}

	public void setMove(Map<String, List<String>> move) {
		this.move = move;
	}

	/**
	 * Executes SQL Commands, Unthreaded in Implementation here
	 */
	public void execute() {
		
		//check variables to see if they are actually system related
		

		if (sql != null && sql.size() > 0) {
			for (String cmd : sql) {
				try {
					log.info("Executing: " + cmd);
					this.template.execute(cmd);
				} catch (Exception e) {
					e.printStackTrace();
					if (kill) {
						break;
					}
				}
			}
		}

		if (move != null && move.size() > 0) {
			for (String key : move.keySet()) {
				String schema = key+ DateFormat.getDateInstance(DateFormat.SHORT).format(Calendar.getInstance().getTime());
				template.execute("CREATE SCHEMA " + Properties.getProperty(key) + "_" + Properties.getProperty(schema));
				for (String table : move.get(key)) {
					template.execute("INSERT INTO " + Properties.getProperty(key) + "_" + Properties.getProperty(schema) + "."+ Properties.getProperty(table) + " VALUES(SELECT * FROM " + Properties.getProperty(key) + "."+ Properties.getProperty(table) + ")");
				}
			}
		}

		if (tableMappings != null) {
			for (String tableSQL : tableMappings.keySet()) {
				int lp = 0;
				String sql = null;
				String tableSelect = tableMappings.get(tableSQL);

				if (idval != null && idval.toLowerCase().trim().contains("property:")) {
					idval = System.getProperty(idval.split(":")[1].trim());
				}

				if (tableColumn.toLowerCase().trim().contains("property:")) {
					tableColumn = System.getProperty(tableColumn.split(":")[1].trim());
				}

				if (tableSchema != null
						&& tableSchema.toLowerCase().contains("property:")) {
					tableSchema = System.getProperty(tableSchema.split(":")[1]
							.trim());
				}

				if (tableIdentifier != null
						&& tableIdentifier.toLowerCase().contains("property:")) {
					tableIdentifier = System.getProperty(tableIdentifier
							.split(":")[1].trim());
				}

				if (tableSelect.trim().toLowerCase().contains("property:")) {
					sql = "SELECT * FROM "+ System.getProperty(tableSelect.split(":")[1])+ " WHERE " + tableIdentifier + " LIKE '" + idval+ "'";
				} else {
					sql = tableSelect;
				}

				// log.info("Executing "+sql);

				for (String record : template.getJsonData(sql)) {
					JsonObject tj = JsonObject.readFrom(record);
					if (schemaColumn != null) {
						tableSchema = tj.get(schemaColumn).asString();
					}

					if (tj.get(tableColumn) != null
							&& tableSchema.trim().length() > 0) {
						String temp = Properties.getProperty(tableSQL.replace("$SCHEMA$", tableSchema));
						temp = Properties.getProperty(temp.replace("$TABLE$", tj.get(tableColumn).asString().trim()));
						log.info("Executing " + temp);

						if (lp > 0 && replaces != null) {
							for (String key : replaces.keySet()) {
								log.info(key + " " + replaces.get(key));
								temp = temp.replaceAll(key, replaces.get(key));
								log.info(temp);
							}
							log.info("Final SQL " + temp);
						}

						template.execute(temp);
					} else {
						log.warn("Table name was problematic.");
					}
					lp += 1;
				}

				log.info("Tables Changed: " + lp);
			}
		}
	}

}

package com.hygenics.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

import mjson.Json;

import org.apache.commons.vfs2.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

import com.hygenics.exceptions.DataCountException;
import com.hygenics.exceptions.InvalidPath;
import com.hygenics.exceptions.MissingData;

/**
 * This is an insertion script. It inserts table counts and column counts for a
 * given schema. I decided to separately look for and report on data drops that
 * may not be within the acceptable parameters in a more in depth reporting
 * mechanism via Python and Pandas. This will save on emails and provide an
 * easier way to summarize data. The reports should run weekly to be most
 * effective.
 * 
 * Some basic QA processes are included though. Photographs are checked for,
 * and, if there is an
 * 
 * I am getting inundated with emails and this is causing some issues.
 * 
 * @author asevans48
 */
public class QualityAssurer {

	private static final Logger log = LoggerFactory.getLogger(MainApp.class);
	private getDAOTemplate template;

	private long termtime = 5000;
	private int procnum = 2;
	private int qnum = Runtime.getRuntime().availableProcessors();
	private String notnull = null;
	
	private List<String> oneMustMatch;
	private String filenameRegex;
	private String dropFolder;
	private String sourceTable;
	private String countTable;
	private String columnCheckTable;
	private String schema;
	private String photoFolder;
	private String photoTable;
	private String imageReplace = "(?mis)\\.jpg";
	private double imageCutoff = -1;

	@NotNull
	private String imageColumn = "image_path";

	
	
	public List<String> getOneMustMatch() {
		return oneMustMatch;
	}

	public void setOneMustMatch(List<String> oneMustMatch) {
		this.oneMustMatch = oneMustMatch;
	}

	// Getters and Setters
	public String getCountTable() {
		return countTable;
	}

	public String getDropFolder() {
		return dropFolder;
	}

	public void setDropFolder(String dropFolder) {
		this.dropFolder = dropFolder;
	}

	public String getFilenameRegex() {
		return filenameRegex;
	}

	public void setFilenameRegex(String filenameRegex) {
		this.filenameRegex = filenameRegex;
	}

	public double getImageCutoff() {
		return imageCutoff;
	}

	public void setImageCutoff(double imageCutoff) {
		this.imageCutoff = imageCutoff;
	}

	public String getImageColumn() {
		return imageColumn;
	}

	public void setImageColumn(String imageColumn) {
		this.imageColumn = imageColumn;
	}

	public String getImageReplace() {
		return imageReplace;
	}

	public void setImageReplace(String imageReplace) {
		this.imageReplace = imageReplace;
	}

	public String getPhotoTable() {
		return photoTable;
	}

	public void setPhotoTable(String photoTable) {
		this.photoTable = photoTable;
	}

	public void setCountTable(String countTable) {
		this.countTable = countTable;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	@Autowired
	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	@Required
	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getPhotoFolder() {
		return photoFolder;
	}

	public void setPhotoFolder(String photoFolder) {
		this.photoFolder = photoFolder;
	}

	public String getColumnCheckTable() {
		return columnCheckTable;
	}

	@Required
	public void setColumnCheckTable(String columnCheckTable) {
		this.columnCheckTable = columnCheckTable;
	}

	@Required
	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	private void checkTable() {
		// check that the tables and schemas exist
		if (this.columnCheckTable != null) {
			log.info("Checking " + this.columnCheckTable);
			String[] splarr = this.columnCheckTable.split("\\.");
			if (!this.template.checkSchema(splarr[0])) {
				log.info("Creating Schema " + splarr[0]);
				this.template.createSchema(splarr[0]);
			}

			if (!this.template.checkTable(this.columnCheckTable, splarr[0])) {
				this.template
						.execute("CREATE TABLE IF NOT EXISTS "
								+ this.columnCheckTable
								+ " (schemaname varchar(128),tablename varchar(128),columnname varchar(128),count integer, date TIMESTAMP DEFAULT now(),constraint column_count_pk PRIMARY KEY(schemaname,tablename,columnname,date))");
				this.template
						.execute("DROP INDEX IF EXISTS column_count_index");
				this.template.execute("CREATE column_count_index ON "
						+ this.columnCheckTable
						+ " (schemaname,tablename,columnname)");
			}
		}

		if (this.countTable != null) {
			log.info("Checking " + this.countTable);
			String[] splarr = this.countTable.split("\\.");
			if (!this.template.checkSchema(splarr[0])) {
				log.info("Checking " + this.countTable);
				this.template.createSchema(splarr[0]);
			}

			if (!this.template.checkTable(this.countTable, splarr[0])) {
				this.template
						.execute("CREATE TABLE IF NOT EXISTS "
								+ this.countTable
								+ " (schemaname varchar(128),tablename varchar(128),count integer, date TIMESTAMP DEFAULT now(),constraint table_count_pk PRIMARY KEY(schemaname,tablename,date))");
				this.template.execute("DROP INDEX IF EXISTS table_count_index");
				this.template.execute("CREATE INDEX table_count_index ON "
						+ this.countTable + " (schemaname,tablename)");
			}
		}

		if (this.sourceTable != null) {
			log.info("Checking " + this.sourceTable);
			String[] splarr = this.sourceTable.split("\\.");
			if (!this.template.checkSchema(splarr[0])) {
				log.info("Creating Schema " + splarr[0]);
				this.template.createSchema(splarr[0]);
			}

			if (!this.template.checkTable(this.sourceTable, splarr[0])) {
				this.template
						.execute("CREATE TABLE IF NOT EXISTS "
								+ this.sourceTable
								+ " (schemaname varchar(128),date TIMESTAMP DEFAULT now(),constraint source_count_pk PRIMARY KEY(schemaname,date))");
				this.template
						.execute("DROP INDEX IF EXISTS source_count_index");
				this.template.execute("CREATE INDEX source_count_index ON "
						+ this.sourceTable + " (schemaname)");
			}
		}

	}// checkTable

	private class SplitPost extends RecursiveAction {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5942536165467154211L;
		private final getDAOTemplate template;
		private ArrayList<String> json;

		public SplitPost(final getDAOTemplate template,
				final ArrayList<String> json) {
			this.json = json;
			this.template = template;
		}

		@Override
		protected void compute() {
			String table = null;
			ArrayList<String> outJson = new ArrayList<String>();
			for (String jstring : json) {

				Map<String, Json> jmap = Json.read(jstring).asJsonMap();
				if (table == null
						|| !jmap.get("table").asString().equals(table)) {
					if (table != null) {
						log.info("Posting to " + table);
						this.template.postJsonDatawithTable(outJson);
						outJson.clear();
					}
					table = jmap.get("table").asString();
				}
				outJson.add(jstring);
			}

			if (outJson.size() > 0) {
				log.info("Posting to " + table);
				this.template.postJsonDatawithTable(outJson);
			}
		}
	}

	private void sendToDb(ArrayList<String> json, boolean split) {
		if (json.size() > 0)
			log.info("Records to Add: " + json.size());

		if (split) {

			ForkJoinPool f2 = new ForkJoinPool((Runtime.getRuntime()
					.availableProcessors() + ((int) Math.ceil(procnum * qnum))));
			ArrayList<String> l;
			int size = (int) Math.ceil(json.size() / qnum);
			for (int conn = 0; conn < qnum; conn++) {
				l = new ArrayList<String>();
				if (((conn + 1) * size) < json.size()) {
					l.addAll(json.subList((conn * size), ((conn + 1) * size)));

				} else {
					l.addAll(json.subList((conn * size), (json.size() - 1)));
					f2.execute(new SplitPost(template, l));

					break;
				}

				f2.execute(new SplitPost(template, l));
			}

			try {
				f2.awaitTermination(termtime, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

			f2.shutdown();

			int incrementor = 0;

			while (f2.isShutdown() == false && f2.getActiveThreadCount() > 0
					&& f2.isQuiescent() == false) {
				incrementor++;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				log.info("Shutting Down" + incrementor);
			}

			l = null;
			f2 = null;

		} else {
			for (String j : json) {

				boolean valid = false;

				try {
					Json.read(j);
					valid = true;
				} catch (Exception e) {
					log.info("ERROR: JSON NOT FORMATTED PROPERLY");
					System.out.println(j);
				}

				try {

					this.template.postSingleJson(j);
				} catch (Exception e) {
					log.info("Failed to Post");
					log.error(j);
					e.printStackTrace();
				}
			}
		}

	}

	// driver method
	public void run() {
		this.checkTable();
		// CHECK THAT VARIABLES WHERE specified Correctly
		if (this.photoFolder != null || this.photoTable != null) {

			if (this.photoFolder != null ^ this.photoTable != null) {
				log.error("When specifiying a Photo Table or Photo Folder both variables must be present");
				throw new NullPointerException(
						"Photo Table or Photo Folder is missing");
			} else {
				int exists = 0;
				int nonexistant = 0;

				// check that the photos folder matches the photo database
				for (String f : new File(this.photoFolder).list()) {
					if (this.template.getJsonData(
							"SELECT * FROM " + this.photoTable + " WHERE "
									+ this.imageColumn + " ILIKE '%"
									+ f.replaceAll(this.imageReplace, "")
									+ "%'").size() > 0) {
						exists++;
					} else {
						nonexistant++;
					}
				}

				if (exists == 0
						|| (imageCutoff > -1 && nonexistant > 0 && exists
								/ nonexistant < imageCutoff)) {
					try {
						throw new DataCountException(
								"Photo Counts too Low!\nRation: "
										+ Double.toString(exists / nonexistant));
					} catch (DataCountException e) {
						e.printStackTrace();
						System.exit(-1);
					}
				}
			}
		}
		
		java.sql.Timestamp date = new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis());

		File[] files = new File(this.dropFolder).listFiles();
		//check that a file that at least one of the following is present
		if(oneMustMatch != null){
			for(String r : oneMustMatch){
				Pattern p = Pattern.compile(r);
				boolean found = false;
				for(File f : files){
					Matcher m = p.matcher(f.getName());
					if(m.find()){
						found = true;
					}
				}
				if(!found){
					try{
						throw new FileNotFoundException("The file "+r+" Must Be Present!");
					}catch(FileNotFoundException e){
						e.printStackTrace();
						System.exit(-1);
					}
				}
			}
		}
		
		
		// check file naming convention
		if (this.filenameRegex != null || this.dropFolder != null) {
			if (this.filenameRegex != null ^ this.dropFolder != null) {
				log.error("If Drop Folder is Specified or File Name Regex is specified, the other must also be specified.");
				throw new NullPointerException(
						"Drop Folder or File Name Regex Not Specified");
			}

			for (File f : files) {
				if (f.isFile()
						&& !f.getName().contains(".zip")
						&& f.getName().replaceAll(filenameRegex, "").length() != 0) {
					log.error("File Name Was Not Appropriately Specified");
					try {
						throw new InvalidPath(
								"File Name Not Specified Properly. " + f
										+ " must conform to "
										+ this.filenameRegex);
					} catch (InvalidPath e) {
						e.printStackTrace();
						System.exit(-1);
					}
				}
			}

		}

		// Insert Column Counts into QA Table
		int recCount = 0;
		ArrayList<String> jsons = this.template
				.getJsonData("SELECT table_schema as schema,table_name as table,column_name as column FROM information_schema.columns WHERE table_schema LIKE '"
						+ this.schema + "'");
		ArrayList<String> tables = new ArrayList<String>();

		if (jsons.size() > 0) {
			// get column count
			for (String json : jsons) {
				Map<String, Json> jmap = Json.read(json).asJsonMap();

				if (!tables.contains(jmap.get("table").asString())) {
					// get table counts and add to appropriate jsons
					String tquery = "SELECT count(*) FROM "
							+ jmap.get("schema").asString() + "."
							+ jmap.get("table").asString();
					int tcount = Json
							.read(this.template.getJsonData(tquery).get(0))
							.asJsonMap().get("count").asInteger();
					recCount += tcount;
					this.template.execute("INSERT INTO " + this.countTable
							+ "(schemaname,tablename,count) VALUES('"
							+ jmap.get("schema").asString() + "','"
							+ jmap.get("table").asString() + "','" + tcount
							+ "')");
					tables.add(jmap.get("table").asString());
				}

				String query = "SELECT count(\"" + jmap.get("column").asString()
						+ "\") FROM " + jmap.get("schema").asString() + "."
						+ jmap.get("table").asString() + " WHERE \""
						+ jmap.get("column").asString()
						+ "\" IS NOT NULL AND length(trim(cast(\""
						+ jmap.get("column").asString() + "\" as text))) > 0";
				int count = Json.read(this.template.getJsonData(query).get(0))
						.asJsonMap().get("count").asInteger();
				this.template.execute("INSERT INTO " + this.columnCheckTable
						+ "(schemaname,tablename,columnname,count) VALUES('"
						+ jmap.get("schema").asString() + "','"
						+ jmap.get("table").asString() + "','"
						+ jmap.get("column").asString() + "','" + count + "')");
			}

			this.template
					.execute("INSERT INTO " + this.sourceTable + " VALUES('"
							+ this.schema.replaceAll("[0-9]+", "") + "')");

		}

		// if no records are discovered, error
		if (recCount == 0) {
			log.error("No Data Found in Tables. Terminating!");
			try {
				throw new MissingData("No Data Found In Tables");
			} catch (MissingData e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		// If all checks succeed, insert into the source table
		this.template.execute("INSERT INTO " + this.sourceTable + " VALUES('"
				+ this.schema + "')");

	}// run
}
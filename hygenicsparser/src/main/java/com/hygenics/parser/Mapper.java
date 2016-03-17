package com.hygenics.parser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import mjson.Json;

import com.hygenics.scala.FileOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;


/**
 * Map the files in a directory to a Database.
 * 
 * The class is intended for such tasks as discovering which images remain after deduplication.
 * 
 * @author aevans
 */
public class Mapper {

	private static final Logger log = LoggerFactory.getLogger(MainApp.class);
	private getDAOTemplate template;
	private String matches;
	private String directory;
	private ArrayList<String> jsons;
	private String hashCol;
	private String mapCol;
	private String replacementPattern;
	private Boolean recursive = true;
	private String table;
	private int qnum = 5;
	private String select;
	private String notnull;
	private String pathName;
	private Boolean delFiles = false;
	private int commitSize = 100;
	private Long termtime = 30000L;
	
	
	
	public int getCommitSize() {
		return commitSize;
	}

	public void setCommitSize(int commitSize) {
		this.commitSize = commitSize;
	}

	public Boolean getDelFiles() {
		return delFiles;
	}

	public void setDelFiles(Boolean delFiles) {
		this.delFiles = delFiles;
	}

	public String getMatches() {
		return matches;
	}

	public void setMatches(String matches) {
		this.matches = matches;
	}

	public Long getTermtime() {
		return termtime;
	}

	public void setTermtime(Long termtime) {
		this.termtime = termtime;
	}

	public String getPathName() {
		return pathName;
	}

	public void setPathName(String pathName) {
		this.pathName = pathName;
	}

	public String getNotnull() {
		return notnull;
	}

	public void setNotnull(String notnull) {
		this.notnull = notnull;
	}

	public int getQnum() {
		return qnum;
	}

	public void setQnum(int qnum) {
		this.qnum = qnum;
	}

	public String getSelect() {
		return select;
	}

	public void setSelect(String select) {
		this.select = select;
	}

	public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public ArrayList<String> getJsons() {
		return jsons;
	}

	public void setJsons(ArrayList<String> jsons) {
		this.jsons = jsons;
	}

	public String getHashCol() {
		return hashCol;
	}

	public void setHashCol(String hashCol) {
		this.hashCol = hashCol;
	}

	public String getMapCol() {
		return mapCol;
	}

	public void setMapCol(String mapCol) {
		this.mapCol = mapCol;
	}

	public String getReplacementPattern() {
		return replacementPattern;
	}

	public void setReplacementPattern(String replacementPattern) {
		this.replacementPattern = replacementPattern;
	}

	public Boolean getRecursive() {
		return recursive;
	}

	public void setRecursive(Boolean recursive) {
		this.recursive = recursive;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public static Logger getLog() {
		return log;
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
			String val = null;
			String table = null;
			String sql = null;
			ArrayList<String> outlist = new ArrayList<String>();
			int numvals = 0;

			if (this.json != null) {

				for (String str : this.json) {
				
					Map<String, Json> jmap = Json.read(str).asJsonMap();

					if (table == null){
						Set<String> keys = jmap.keySet();
						Iterator<String> it = keys.iterator();
						String fname = null;

						// create statement (a repeat due to comparison against
						// a null condition [just to be careful null is used])
						table = jmap.get("table").asString().trim();
						sql = "INSERT INTO " + table + " (";
						val = "VALUES(";

						// hopefully a smaller loop saves time
						for (int vals = 0; vals < keys.size(); vals++) {
							fname = it.next();

							if (fname.compareTo("table") != 0) {
								if (vals > 0) {
									val += ",";
									sql += ",";
								}

								val += "?";
								sql += fname;
							}
						}

						numvals = keys.size();
						sql += ")";
						val += ")";

						sql = sql + " " + val;

						if (str.trim().compareTo("NO DATA") != 0){
							if (notnull != null) {
								if (jmap.get(notnull.trim()).asString().trim()
										.length() > 0) {
									outlist.add(str);
								}
							}else{
								outlist.add(str);
							}
						}
					} else if(table.compareTo(jmap.get("table").asString().trim()) != 0 || jmap.size() != numvals ) {
						// case is that table is different or the number of
						// values differs which would cause sql to throw an
						// error
						Set<String> keys = jmap.keySet();
						Iterator<String> it = keys.iterator();
						String fname = null;

						// send current data if the waiting list is greater than
						// 0
						if (outlist.size() > 0) {
							this.template.postJsonData(sql, outlist);
							outlist = new ArrayList<String>();
						}

						// reset information
						table = jmap.get("table").asString().trim();
						sql = "INSERT INTO " + table + " (";
						val = "VALUES(";

						// hopefully a smaller loop saves time
						for (int vals = 0; vals < keys.size(); vals++) {
							fname = it.next();

							if (fname.compareTo("table") != 0) {
								if (vals > 0) {
									val += ",";
									sql += ",";
								}

								val += "?";
								sql += fname;
							}
						}

						sql += ")";
						val += ")";

						numvals = keys.size();

						sql = sql + " " + val;
						if (str.trim().compareTo("NO DATA") != 0) {

							if (notnull != null) {
								if (jmap.get(notnull.trim()).asString().trim()
										.length() > 0) {
									outlist.add(str);
								}
							} else {
								outlist.add(str);
							}
						}
					} else {
						// case is that no table is different and the number of
						// values does not differ from the previous ammount

						if (str.trim().compareTo("NO DATA") != 0) {

							if (notnull != null) {
								if (jmap.get(notnull.trim()).asString().trim().length() > 0) {
									outlist.add(str);
								}
							} else {
								outlist.add(str);
							}
						}
					}

					jmap = null;
				}

				// send remaining strings to db
				if (outlist.size() > 0) {

					this.template.postJsonData(sql, outlist);
				}

				sql = null;
				val = null;
				outlist = null;
				json = null;
			}
		}

	}

	private void sendToDb(List<String> json, boolean split) {
		if (json.size() > 0){
			log.info("Records to Add: " + json.size());
	
			if (split) {
	
				ForkJoinPool f2 = new ForkJoinPool(Runtime.getRuntime().availableProcessors() * qnum);
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
						// TODO Auto-generated catch block
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
	}
	
	/**
	 * Split Query. Practice==OK. A lot of things are more understood after
	 * actual use.
	 * 
	 * @author aevans
	 *
	 */
	private class SplitQuery implements Callable<ArrayList<String>> {
		private static final long serialVersionUID = -8185035798235011678L;
		private final getDAOTemplate template;
		private String sql;

		private SplitQuery(final getDAOTemplate template, final String sql) {
			this.template = template;
			this.sql = sql;
		}

		@Override
		public ArrayList<String> call() {
			ArrayList<String> results = template.getJsonData(sql);
			return results;
		}
	}

	/**
	 * Run the Mapper.
	 * 
	 */
	public void run() {
		log.info("Starting Mapping @ "+ Calendar.getInstance().getTime().toString());
		log.info("Table Preparation");
		String[] tarr= table.split("\\.");
		
		if(!this.template.checkSchema(tarr[0])){
			this.template.execute("CREATE SCHEMA IF NOT EXISTS "+tarr[0]);
		}
		
		this.template.execute("DROP TABLE IF EXISTS "+table+" CASCADE");
	
		if(!this.template.checkTable(table,tarr[0])){
			String sql="CREATE TABLE IF NOT EXISTS "+table+"("+hashCol+" text, date TIMESTAMP default now()";
			
			if(!hashCol.equals(mapCol)){
				sql+=", "+mapCol+" text";
			}
			
			if(pathName != null){
				sql +=", "+pathName+" text";
			}
			sql +=")";
			
			this.template.execute(sql);
		}
		
		
		log.info("Getting Data");
		ArrayList<String> jsons = this.template.getJsonData(select);
		
		log.info("Mapping");
		FileOps fops = new FileOps();
		ArrayList<String> arr = fops.getLinkedDirectoryIntersection(matches,directory, jsons, hashCol, mapCol, replacementPattern, recursive, table,pathName,delFiles);
		
		log.info("Sending to DB");
		if(arr.size() > 0){
			int start=0;
			int end=commitSize*qnum;
			while(start <= arr.size()){
				if(start < arr.size()){
					if(end > arr.size()){
						end = arr.size();
					}
					this.sendToDb(arr.subList(start, end),true);
					start += commitSize * qnum;
					end += commitSize * qnum;
				}
			}
		}
		
		log.info("Finished Mapping @ "+ Calendar.getInstance().getTime().toString());
	}
}
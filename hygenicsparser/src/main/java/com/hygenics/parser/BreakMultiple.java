package com.hygenics.parser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import mjson.Json;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Breaks apart Records with Multiple Entries. This may be the result of having
 * run a looped or multiregex query. Also performs Replacements. This class
 * works for one table. Utilizes split queries.
 * 
 * I found that using the DAO template and bonepc pool has the same effect as
 * pooling jdbc connections and running more connections.About ~22.4 seconds
 * (22.35 average) for ~12000 posts and ~1000 pulls along with parsing using
 * ~1700 regex operations, all with fork/join pools. Don't go too high. I
 * swamped the server with connections. It hates me for a while.
 * 
 * You can specify a primary key and foreign key in this step if desired. The
 * primary key is usesful in pre-processing data for duplicate and other
 * matching before being sent out (statistically and comparatively removing
 * duplicates as opposed to grouping and comparing which is an improvement over
 * the existing tool since these sources are incredibly unclean).
 *
 * 
 * @author aevans
 *
 */
public class BreakMultiple {

	private boolean unescape = false;
	private boolean truncate = true;
	private boolean cascade = false;

	private String extracondition;
	private String fkeyref;

	private String key;
	private String foreignkey;

	private String notnull;
	private int pullsize = 100;
	private boolean genhash = false;
	private boolean repeatkeys = false;
	private int loops = 0;
	private int waitloops = 0;

	private String mustcontain;
	private String cannotcontain;

	private static final Logger log = LoggerFactory.getLogger(MainApp.class);
	private int SPLITSIZE = 100;

	private int termtime = 500;

	private int sqlnum = 1;
	private int procnum = 2;
	// row column
	private String rowcolumn;

	private int qnum = 5;

	// maximum position for each db entry
	private int maxpos;

	// id column
	private String idcolumn;

	// offset to start pulling records from
	private int offset;

	// commit size
	private int commit_size = 100;

	// getDAO
	private getDAOTemplate template;

	// select statement
	private String select;

	// target table
	private String targettable;

	// replacement pattersn to be replaced with nill
	private String replacementPattern;

	// the array list containing the tables to take from and a mapping of
	// positions and target variables
	private Map<String, String> positions;

	// data string
	private ArrayList<String> rows;

	// token to split on
	private String token = "\\|";

	public BreakMultiple() {

	}

	public boolean isUnescape() {
		return unescape;
	}

	public void setUnescape(boolean unescape) {
		this.unescape = unescape;
	}

	public String getFkeyref() {
		return fkeyref;
	}

	public void setFkeyref(String fkeyref) {
		this.fkeyref = fkeyref;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getForeignkey() {
		return foreignkey;
	}

	public void setForeignkey(String foreignkey) {
		this.foreignkey = foreignkey;
	}

	public boolean isTruncate() {
		return truncate;
	}

	public void setTruncate(boolean truncate) {
		this.truncate = truncate;
	}

	public boolean isCascade() {
		return cascade;
	}

	public void setCascade(boolean cascade) {
		this.cascade = cascade;
	}

	private class GetFromDB implements Callable<ArrayList<String>> {
		private final getDAOTemplate template;
		private final String sql;

		public GetFromDB(final String sql, final getDAOTemplate template) {
			super();
			this.template = template;
			this.sql = sql;
		}

		public ArrayList<String> call() {
			return this.template.getJsonData(sql);
		}

	}

	private class SplitPost implements Runnable {
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

	
		public void run() {
			// TODO Auto-generated method stub
			String val = null;
			String table = null;
			String sql = null;
			ArrayList<String> outlist = new ArrayList<String>();
			int numvals = 0;
			boolean missingkey = false;

			if (this.json != null) {

				Set<String> keys = null;
				for (String str : this.json) {
					Map<String, Json> jmap = Json.read(str).asJsonMap();

					// check that all keys are present
					if (table != null & keys != null) {
						for (String k : keys) {
							if (jmap.containsKey(k) == false) {
								missingkey = true;
							}
						}
					}
					keys = jmap.keySet();

					if (table == null) {
						// CASE IS THAT no table has been added

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
						log.info(sql);
						if (notnull != null) {
							if (jmap.get(notnull.trim()).asString().trim()
									.length() > 0) {
								outlist.add(str);
							}
						} else {
							outlist.add(str);
						}
					} else if (table.compareTo(jmap.get("table").asString()
							.trim()) != 0
							| jmap.size() > numvals
							| jmap.size() < numvals
							| missingkey == true) {
						// case is that table is different or the number of
						// values differs which would cause sql to throw an
						// error
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
						if (notnull != null) {
							if (jmap.get(notnull.trim()).asString().trim()
									.length() > 0) {
								outlist.add(str);
							}
						} else {
							outlist.add(str);
						}
					} else {
						// case is that no table is different and the number of
						// values does not differ from the previous ammount
						if (notnull != null) {
							if (jmap.get(notnull.trim()).asString().trim()
									.length() > 0) {
								outlist.add(str);
							}
						} else {
							outlist.add(str);
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

	/**
	 * Break Multiple records or pages apart, takes in genhash as an argument
	 * 
	 * @author aevens
	 *
	 */
	private class Break implements Callable<ArrayList<String>> {
		private final boolean unescape;
		private boolean genhash;
		private final getDAOTemplate template;
		private String row;
		private final String token;
		private final String replacementPattern;
		private Map<String, String> positions;
		private final String date;
		private String offenderhash;
		private final int maxpos;
		private final String table;
		private final boolean repeatkeys;

		/**
		 * Constructor
		 * 
		 * @param unescape
		 * @param repeatkeys
		 * @param template
		 * @param row
		 * @param token
		 * @param replacementPattern
		 * @param positions
		 * @param date
		 * @param table
		 * @param offenderhash
		 * @param maxpos
		 * @param genhash
		 */
		public Break(final boolean unescape, final boolean repeatkeys,
				final getDAOTemplate template, final String row,
				final String token, final String replacementPattern,
				final Map<String, String> positions, final String date,
				final String table, String offenderhash, final int maxpos,
				final boolean genhash) {
			this.unescape = unescape;
			this.maxpos = maxpos;
			this.token = token;
			this.template = template;
			this.row = row;
			this.replacementPattern = replacementPattern;
			this.positions = positions;
			this.date = date;
			this.offenderhash = offenderhash;
			this.table = table;
			this.repeatkeys = repeatkeys;
			this.genhash = genhash;
		}

		/**
		 * Generates a Unique hash specific to the pull
		 * 
		 * @param id
		 * @return
		 */
		private String genHash(String id) {

			long h = 0;
			String d = Long.toString((Calendar.getInstance().getTimeInMillis())
					* (long) (Math.random() * 10));

			for (int i = 0; i < d.length(); i++) {
				h += (d.charAt(i) * (10 * i));
			}

			return Long.toString(h) + id;
		}

		
		public ArrayList<String> call() {
			int id = 0;
			String json = null;
			ArrayList<String> retrows = new ArrayList<String>();
			String r = this.row;

			if (this.unescape) {
				r = StringEscapeUtils.unescapeHtml3(r);
			}

			String[] vars = this.row.split(token);
			String k = null;
			int j = 0;

			if (positions != null) {

				for (int i = 0; i < vars.length; i++) {

					if ((i % this.maxpos) == 0 & i != 0) {
						id += 12;
						if (genhash == true) {
							offenderhash = genHash(Integer.toString(id));
						}

						if (json != null) {
							json += ",\"table\":\"" + table + "\",\"date\":\""
									+ date + "\",\"offenderhash\":\""
									+ offenderhash + "\"}";
							retrows.add(json);
						}

						json = null;
					}

					if ((j >= positions.size() & repeatkeys == true)
							| j < positions.size()) {

						j = (j >= positions.size()) ? 0 : j;

						if (positions.containsKey(Integer.toString(j).trim())) {
							// positions contains key so add
							if (positions.get(Integer.toString(j)).trim()
									.toLowerCase().contains("skip") == false) {
								json = (json == null) ? "{\""
										+ positions
												.get(Integer.toString(j).trim())
												.replaceAll(
														(replacementPattern + "|\""),
														"")
										+ "\":\""
										+ vars[i].replaceAll(
												(replacementPattern + "|\""),
												"") + "\""
										: json
												+ ",\""
												+ positions
														.get(Integer
																.toString(j)
																.trim())
														.replaceAll(
																(replacementPattern + "|\""),
																"")
												+ "\":\""
												+ vars[i]
														.replaceAll(
																(replacementPattern + "|\""),
																"") + "\"";
							}
							j++;
						} else if (repeatkeys == true) {
							// position should be repeated
							j = 0;
							if (positions.get(Integer.toString(j)).trim()
									.toLowerCase().contains("skip") == false) {
								json = (json == null) ? "{\""
										+ positions
												.get(Integer.toString(j).trim())
												.replaceAll(
														(replacementPattern + "|\""),
														"")
										+ "\":\""
										+ vars[i].replaceAll(
												(replacementPattern + "|\""),
												"") + "\""
										: json
												+ ",\""
												+ positions
														.get(Integer
																.toString(j)
																.trim())
														.replaceAll(
																(replacementPattern + "|\""),
																"")
												+ "\":\""
												+ vars[i]
														.replaceAll(
																(replacementPattern + "|\""),
																"") + "\"";
							}
							j++;
						}

						if (i == vars.length - 1) {
							if (json != null) {
								id++;
								if (genhash == true) {
									offenderhash = genHash(Integer.toString(id));
								}

								json += ",\"table\":\"" + table
										+ "\",\"date\":\"" + date
										+ "\",\"offenderhash\":\""
										+ offenderhash + "\"}";
								retrows.add(json);
							}

							json = null;
						}
					}

				}
			}

			row = null;
			positions = null;
			return retrows;
		}

	}

	public String getExtracondition() {
		return extracondition;
	}

	public void setExtracondition(String extracondition) {
		this.extracondition = extracondition;
	}

	/**
	 * Get the user specified field that cannot be null. This is used in posting
	 * data to the database, specifically in the parallel post class.
	 * 
	 * @return
	 */
	public String getNotnull() {
		return notnull;
	}

	/**
	 * Set the user specified field stating what cannot be null. This is used in
	 * posting data to the database, specifically in the parallel post class.
	 * 
	 * @param notnull
	 */
	public void setNotnull(String notnull) {
		this.notnull = notnull;
	}

	public int getPullsize() {
		return pullsize;
	}

	public void setPullsize(int pullsize) {
		this.pullsize = pullsize;
	}

	public boolean isGenhash() {
		return genhash;
	}

	public void setGenhash(boolean genhash) {
		this.genhash = genhash;
	}

	public boolean isRepeatkeys() {
		return repeatkeys;
	}

	public void setRepeatkeys(boolean repeatkeys) {
		this.repeatkeys = repeatkeys;
	}

	public String getMustcontain() {
		return mustcontain;
	}

	public void setMustcontain(String mustcontain) {
		this.mustcontain = mustcontain;
	}

	public String getCannotcontain() {
		return cannotcontain;
	}

	public void setCannotcontain(String cannotcontain) {
		this.cannotcontain = cannotcontain;
	}

	public int getLoops() {
		return loops;
	}

	public void setLoops(int loops) {
		this.loops = loops;
	}

	public int getWaitloops() {
		return waitloops;
	}

	public void setWaitloops(int waitloops) {
		this.waitloops = waitloops;
	}

	public int getTermtime() {
		return termtime;
	}

	public void setTermtime(int termtime) {
		this.termtime = termtime;
	}

	public int getSPLITSIZE() {
		return SPLITSIZE;
	}

	public void setSPLITSIZE(int sPLITSIZE) {
		SPLITSIZE = sPLITSIZE;
	}

	public int getSqlnum() {
		return sqlnum;
	}

	public void setSqlnum(int sqlnum) {
		this.sqlnum = sqlnum;
	}

	public int getProcnum() {
		return procnum;
	}

	public void setProcnum(int procnum) {
		this.procnum = procnum;
	}

	public int getQnum() {
		return qnum;
	}

	public void setQnum(int qnum) {
		this.qnum = qnum;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public int getMaxpos() {
		return maxpos;
	}

	public void setMaxpos(int maxpos) {
		this.maxpos = maxpos;
	}

	public String getSelect() {
		return select;
	}

	@Required
	public void setSelect(String select) {
		this.select = select;
	}

	public String getTargettable() {
		return targettable;
	}

	@Required
	public void setTargettable(String targettable) {
		this.targettable = targettable;
	}

	public String getRowcolumn() {
		return rowcolumn;
	}

	@Required
	public void setRowcolumn(String rowcolumn) {
		this.rowcolumn = rowcolumn;
	}

	public String getIdcolumn() {
		return idcolumn;
	}

	@Required
	public void setIdcolumn(String idcolumn) {
		this.idcolumn = idcolumn;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getCommit_size() {
		return commit_size;
	}

	public void setCommit_size(int commit_size) {
		this.commit_size = commit_size;
	}

	public String getReplacementPattern() {
		return replacementPattern;
	}

	public void setReplacementPattern(String replacementPattern) {
		this.replacementPattern = replacementPattern;
	}

	public Map<String, String> getPositions() {
		return positions;
	}

	public void setPositions(Map<String, String> positions) {
		this.positions = positions;
	}

	public ArrayList<String> getRows() {
		return rows;
	}

	public void setRows(ArrayList<String> rows) {
		this.rows = rows;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	/**
	 * Post to db
	 * 
	 * @param json
	 * @param split
	 */
	public void postToDb(ArrayList<String> json, boolean split) {
		log.info("Posting " + json.size() + " Records");

		if (split) {

			ForkJoinPool f2 = new ForkJoinPool(
					(Runtime.getRuntime().availableProcessors() + ((int) Math
							.ceil(procnum * sqlnum))));
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
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			f2.shutdown();

			int incrementor = 0;

			while (f2.isShutdown() == false) {
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
			log.info("Singlethread");

			this.template.postJsonDatawithTable(json);
		}

	}

	public void checkTable() {
		String[] tablearr = targettable.split("\\.");

		if (this.template.checkSchema(tablearr[0]) == false) {
			this.template.createSchema(tablearr[0]);
		}

		if (this.template.checkTable(targettable, tablearr[0]) == false) {
			Map<String, String> tpositions = new HashMap<String, String>();

			int j = 0;
			for (String k : positions.keySet()) {
				if (positions.get(k).contains("skip") == false) {
					tpositions.put((Integer.toString(j)), positions.get(k));
					j++;
				}
			}

			this.template.createTablebyMap(targettable, tablearr[0],
					tpositions, key, foreignkey, fkeyref);
		} else if (truncate) {
			String sql = "TRUNCATE " + targettable;

			if (cascade) {
				sql += " CASCADE";
			}
			template.execute(sql);
		}
	}

	/**
	 * run the class
	 */
	public void run() {
		int j = 0;
		checkTable();
		rows = new ArrayList<String>();
		log.info("Starting Break");

		// the pool
		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors() * procnum);

		// for returned results
		List<Future<ArrayList<String>>> futures = new ArrayList<Future<ArrayList<String>>>();

		// for parsing
		Set<Callable<ArrayList<String>>> collect = new HashSet<Callable<ArrayList<String>>>();

		// for querying
		Set<Callable<ArrayList<String>>> qcollect = new HashSet<Callable<ArrayList<String>>>();

		// results
		ArrayList<String> jsons = new ArrayList<String>();

		String condition = null;
		int size = (int) Math.ceil(pullsize / qnum);
		// get initial data from user
		for (int i = 0; i < qnum; i++) {
			condition = " WHERE "
					+ idcolumn
					+ " > "
					+ Integer.toString(offset
							+ (Math.round(pullsize / qnum) * i))
					+ " AND "
					+ idcolumn
					+ " <= "
					+ Integer.toString(offset
							+ (Math.round(pullsize / qnum) * (i + 1)));

			if (extracondition != null) {
				condition += " " + extracondition.trim();
			}

			qcollect.add(new GetFromDB((select + condition), template));
			log.info("SELECTING " + select + " " + condition);
		}

		log.info("Getting From DB @"
				+ Calendar.getInstance().getTime().toString());
		futures = fjp.invokeAll(qcollect);

		int w = 0;
		while (fjp.getActiveThreadCount() > 0 && fjp.isQuiescent() == false) {
			w++;
		}

		log.info("Waited for " + w + "Cycles");

		for (Future<ArrayList<String>> f : futures) {
			try {
				rows.addAll(f.get());
				f.cancel(true);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		qcollect = new HashSet<Callable<ArrayList<String>>>();
		futures = null;

		log.info("Breaking");
		// process while there is still data to process
		while (rows.size() > 0) {
			log.info("Iteration Contains " + rows.size() + " Rows");
			// add to the commit size for future processing
			offset += pullsize;
			log.info("Submitting Tasks");
			// submit for breaking apart

			for (String r : rows) {

				if (fjp.isShutdown()) {
					fjp = new ForkJoinPool(Runtime.getRuntime()
							.availableProcessors() * procnum);
				}

				if (r != null) {

					if (mustcontain != null) {
						if (r.contains(mustcontain)) {
							if (cannotcontain != null) {
								if (r.contains(cannotcontain) == false) {
									Map<String, Json> rowmap = Json.read(r)
											.asJsonMap();

									// final getDAOTemplate template, final
									// String row, final String token, final
									// String replacementPattern, final
									// Map<String,String> positions,final String
									// date, final String table, final String
									// offenderhash
									if (rowmap.size() > 0) {
										collect.add(new Break(unescape,
												repeatkeys, template, rowmap
														.get(rowcolumn)
														.asString(), token,
												replacementPattern, positions,
												(Calendar.getInstance()
														.getTime().toString()),
												targettable, rowmap.get(
														"offenderhash")
														.asString(), maxpos,
												genhash));
									}
								}
							} else {
								Map<String, Json> rowmap = Json.read(r)
										.asJsonMap();

								// final getDAOTemplate template, final String
								// row, final String token, final String
								// replacementPattern, final Map<String,String>
								// positions,final String date, final String
								// table, final String offenderhash
								if (rowmap.size() > 0) {
									collect.add(new Break(unescape, repeatkeys,
											template, rowmap.get(rowcolumn)
													.asString(), token,
											replacementPattern, positions,
											(Calendar.getInstance().getTime()
													.toString()), targettable,
											rowmap.get("offenderhash")
													.asString(), maxpos,
											genhash));
								}
							}
						}
					} else {

						if (cannotcontain != null) {
							if (r.contains(cannotcontain) == false) {
								Map<String, Json> rowmap = Json.read(r)
										.asJsonMap();

								// to ascend you must die, to die you must be
								// crucified; so get off your -- cross so that
								// we can nail down the nex martyr
								// final getDAOTemplate template, final String
								// row, final String token, final String
								// replacementPattern, final Map<String,String>
								// positions,final String date, final String
								// table, final String offenderhash
								if (rowmap.size() > 0) {
									collect.add(new Break(unescape, repeatkeys,
											template, rowmap.get(rowcolumn)
													.asString(), token,
											replacementPattern, positions,
											(Calendar.getInstance().getTime()
													.toString()), targettable,
											rowmap.get("offenderhash")
													.asString(), maxpos,
											genhash));
								}
							}
						} else {
							Map<String, Json> rowmap = Json.read(r).asJsonMap();

							// final getDAOTemplate template, final String row,
							// final String token, final String
							// replacementPattern, final Map<String,String>
							// positions,final String date, final String table,
							// final String offenderhash
							if (rowmap.size() > 0) {
								collect.add(new Break(unescape, repeatkeys,
										template, rowmap.get(rowcolumn)
												.asString(), token,
										replacementPattern, positions,
										(Calendar.getInstance().getTime()
												.toString()), targettable,
										rowmap.get("offenderhash").asString(),
										maxpos, genhash));
							}
						}
					}
				}
			}

			log.info("SUBMITTED " + collect.size() + " tasks");

			futures = fjp.invokeAll(collect);

			w = 0;

			while (fjp.getActiveThreadCount() > 0 && fjp.isQuiescent() == false) {
				w++;
			}

			log.info("Waited for " + w + " Cycles");

			jsons.clear();
			log.info("Getting Strings");
			try {

				for (Future<ArrayList<String>> p : futures) {
					ArrayList<String> retlist = p.get();

					if (retlist != null) {
						if (retlist.size() > 0) {
							jsons.addAll(retlist);
						}

						if (jsons.size() >= commit_size) {
							// send to db
							if (jsons.size() > SPLITSIZE) {
								log.info("Split True: Sending to DB @ "
										+ Calendar.getInstance().getTime()
												.toString());

								postToDb(jsons, true);
								jsons = new ArrayList<String>();
								log.info("Posted to DB @ "
										+ Calendar.getInstance().getTime()
												.toString());
							} else {
								log.info("Split False: Sending to DB @ "
										+ Calendar.getInstance().getTime()
												.toString());
								postToDb(jsons, false);
								jsons = new ArrayList<String>();
								log.info("Posted to DB @ "
										+ Calendar.getInstance().getTime()
												.toString());
							}
						}
					}
					p.cancel(true);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			futures = null;
			collect = new HashSet<Callable<ArrayList<String>>>();

			// send to db
			if (jsons.size() > SPLITSIZE) {
				log.info("Split True: Sending to DB @"
						+ Calendar.getInstance().getTime().toString());
				postToDb(jsons, true);
				jsons = new ArrayList<String>();
				log.info("Posted to DB @ "
						+ Calendar.getInstance().getTime().toString());
			} else {
				log.info("Split False: Sending to DB @"
						+ Calendar.getInstance().getTime().toString());
				postToDb(jsons, false);
				jsons = new ArrayList<String>();
				log.info("Posted to DB @ "
						+ Calendar.getInstance().getTime().toString());
			}

			// get more information
			rows = new ArrayList<String>();

			if (Runtime.getRuntime().freeMemory() < 500000
					| ((loops % waitloops) == 0 & waitloops != 0)) {
				log.info("Paused Free Memory Left: "
						+ Runtime.getRuntime().freeMemory());
				System.gc();
				Runtime.getRuntime().gc();

				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				while (Runtime.getRuntime().freeMemory() < 500000) {
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				log.info("Restart Free Memory Left: "
						+ Runtime.getRuntime().freeMemory());
			}

			rows = new ArrayList<String>();

			// attempt to query the database from multiple threads
			for (int conn = 1; conn <= qnum; conn++) {
				// change condition
				condition = " WHERE "
						+ idcolumn
						+ " > "
						+ Integer.toString(offset
								+ (Math.round(pullsize / qnum) * conn))
						+ " AND "
						+ idcolumn
						+ " <= "
						+ Integer.toString(offset
								+ (Math.round(pullsize / qnum) * (conn + 1)));

				if (extracondition != null) {
					condition += " " + extracondition.trim();
				}

				qcollect.add(new GetFromDB((select + condition), template));
				log.info("SELECTING " + select + " " + condition);
			}

			futures = fjp.invokeAll(qcollect);

			w = 0;

			while (fjp.getActiveThreadCount() > 0 && fjp.isQuiescent() == false) {
				w++;
			}

			log.info("Waited for " + w + " Cycles");

			for (Future<ArrayList<String>> f : futures) {
				try {

					ArrayList<String> test = f.get();

					if (test != null) {
						if (test.size() > 0) {
							rows.addAll(test);
						}
					}

					f.cancel(true);

				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			futures = null;
			qcollect = new HashSet<Callable<ArrayList<String>>>(4);

			j++;

			Runtime.getRuntime().gc();
			System.gc();

		}

		// send to db
		if (jsons.size() > SPLITSIZE) {
			log.info("Split True: Sending to DB @"
					+ Calendar.getInstance().getTime().toString());
			postToDb(jsons, true);
			jsons = new ArrayList<String>();
		} else if (jsons.size() > 0) {
			log.info("Split False: Sending to DB @"
					+ Calendar.getInstance().getTime().toString());
			postToDb(jsons, false);
			jsons = new ArrayList<String>();
		}

		Runtime.getRuntime().gc();
		System.gc();

		log.info("Shutting Down Forkjoin Pool");
		if (fjp.isShutdown() == false) {
			fjp.shutdownNow();
		}
	}

}

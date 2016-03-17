package com.hygenics.parser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import mjson.Json;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.beans.factory.annotation.Autowired;

import com.eclipsesource.json.JsonObject;

/**
 * Parses with Xpath or CSS. But also allows for Regular Expressions to be Used
 * to Narrow Down the Values. When narrowing, an xmlns header is added by
 * default but the header can be respecified.
 * 
 * These classes keep RAM usage down while efficiently using the processor by
 * maximizing garbage collection capabilities, This code ensuring that
 * references to referents are actually broken when not in use, doesn't keep
 * large strings in memory long enough for them to pass to the older generation,
 * utilize StringBuilders to avoid String Pools,etc.
 * 
 * @author aevans
 *
 */
public class ParseJSoup {

	// /VARIABLES///
	private String checkstring;
	private int maxchecks;

	private static Logger log = LoggerFactory.getLogger(MainApp.class.getName());
	private int commitsize = 100;
	private String mustcontain;
	private String cannotcontain;
	private String notnull;
	private String column = "html";
	private int timeout = 10000;
	private String pullid;
	private String header;
	private String footer;
	private String pagenarrow;
	private String targetTable;

	private String replace;
	private String replaceSequence;
	private String extracondition;

	private int offset;
	private String select;

	private int qnums = 4;
	private int procs = 1;

	private Map<String, String> singlepaths;
	private Map<String, Map<String, String>> multipaths;
	private Map<String, Map<String, String>> recordpaths;

	@Autowired
	private getDAOTemplate template;

	// /INNER CLASSES FOR DATABASE ACCESS
	/***
	 * Post Data to the Database Across Multiple Threads. BE CAREFUL.
	 * 
	 * @author aevans
	 *
	 */
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

					if (table == null) {
						// CASE IS THAT no table has been added

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
					} else if (table.compareTo(jmap.get("table").asString()
							.trim()) != 0
							| jmap.size() > numvals | jmap.size() < numvals) {
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
								if (jmap.get(notnull.trim()).asString().trim()
										.length() > 0) {
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

	/**
	 * For Multithreaded Grabbing of Data from a database. This works only
	 * because the proper DAO Template method is set to work asynchronously.
	 * 
	 * @author aevans
	 *
	 */
	private class SplitQuery implements Callable<ArrayList<String>> {
		private final String select;

		SplitQuery(String select) {
			this.select = select;
		}

		@Override
		public ArrayList<String> call() throws Exception {
			// TODO Auto-generated method stub
			return template.getJSonDatawithQuotes(select);
		}
	}

	// /INNER CLASSES FOR FJP///

	/**
	 * Parses a Single Attribute from an HTML Document
	 * 
	 * @author aevans
	 *
	 */
	private class ParseSingle implements Callable<ArrayList<String>> {

		private final String header;
		private final String pagenarrow;
		private final Map<String, String> xpaths;
		private final String html;
		private final String replace;
		private final String replaceSequence;
		private final String offenderhash;
		private final String footer;

		ParseSingle(String offenderhash, String header, String footer,
				String pagenarrow, Map<String, String> xpaths, String html,
				String replace, String replaceSequence) {
			this.header = header;
			this.pagenarrow = pagenarrow;
			this.xpaths = xpaths;
			this.html = html;
			this.replace = replace;
			this.replaceSequence = replaceSequence;
			this.offenderhash = offenderhash;
			this.footer = footer;
		}

		@Override
		public ArrayList<String> call() throws Exception {
			// TODO return the parsed XPath String with narrowing Effects.
			// Single
			String matchhtml = html;
			JsonObject jobject = new JsonObject();
			Document jsoup;
			ArrayList<String> retarr = new ArrayList<String>(1);

			// add the offenderhash and table to the record
			jobject.add("table", xpaths.get("table"));
			jobject.add("offenderhash", this.offenderhash);

			// Limit the HTM and re-add the header
			if (this.pagenarrow != null) {
				Pattern p = Pattern.compile(this.pagenarrow);
				Matcher m = p.matcher(this.html);

				if (m.find()) {
					if (header != null) {
						matchhtml = (m.group().contains(header) == false) ? header
								+ "\n" + m.group()
								: m.group();
					} else {
						matchhtml = m.group();
					}
				}

			}

			jsoup = Jsoup.parse(matchhtml);

			// Get each Attribute and build the appropriate Json String
			for (String key : xpaths.keySet()) {
				if (key.toLowerCase().trim().compareTo("table") != 0) {

					// force error on purpose --> unchecked
					// ArrayIndexOutOfBounds and NullPointerException
					String[] atvals = xpaths.get(key).trim().split("=");
					Elements els = jsoup.getElementsByAttributeValue(atvals[0],
							atvals[1]);

					if (els.size() > 0) {
						jobject.add(
								key.trim(),
								((this.replace != null) ? els
										.get(0)
										.text()
										.trim()
										.replaceAll(this.replace,
												this.replaceSequence) : els
										.get(0).text().trim()));

					} else {
						jobject.add(key.trim(), "");
					}

				}

			}
			matchhtml = null;
			jsoup = null;
			retarr.add(jobject.toString());
			return retarr;
		}
	}

	/**
	 * An Added function that, with the increased integrity of rows with
	 * attributes, parses each row.
	 * 
	 * @author aevans
	 *
	 */
	private class ParseRows implements Callable<ArrayList<String>> {

		private final String header;
		private final String pagenarrow;
		private final Map<String, Map<String, String>> xpaths;
		private final String html;
		private final String replace;
		private final String replaceSequence;
		private final String offenderhash;
		private final String footer;

		ParseRows(String offenderhash, String header, String footer,
				String pagenarrow, Map<String, Map<String, String>> xpaths,
				String html, String replace, String replaceSequence) {
			this.offenderhash = offenderhash;
			this.header = header;
			this.pagenarrow = pagenarrow;
			this.xpaths = xpaths;
			this.html = html;
			this.replace = replace;
			this.replaceSequence = replaceSequence;
			this.footer = footer;
		}

		@Override
		public ArrayList<String> call() throws Exception {
			// TODO Get Records Row By Row

			String matchhtml = html;
			ArrayList<String> jsons = new ArrayList<String>();
			Document jsoup;
			Pattern p;
			Matcher m;
			// Limit the HTM and re-add the header
			if (this.pagenarrow != null) {
				p = Pattern.compile(this.pagenarrow);
				m = p.matcher(this.html);

				if (m.find()) {
					if (header != null) {
						matchhtml = (m.group().contains(header) == false) ? header
								+ "\n" + m.group()
								: m.group();
					} else {
						matchhtml = m.group();
					}

					if (footer != null) {
						matchhtml += footer;
					}
				}

			}

			// Get Rows
			// Input format is Map<table,Map<key,attr>>
			for (String key : xpaths.keySet()) {

				if (xpaths.get(key).containsKey("narrow")) {

					p = Pattern.compile(xpaths.get(key).get("narrow").trim());
					m = p.matcher(matchhtml);

					if (m.find()) {
						if (header != null) {
							matchhtml = (m.group().contains(header)) ? m
									.group() : header + "\n" + m.group();
						} else {
							matchhtml = m.group();
						}

						if (footer != null) {
							matchhtml += footer;
						}
					}

				}

				if (matchhtml != null) {
					if (xpaths.get(key).containsKey("rows")) {

						p = Pattern.compile(xpaths.get(key).get("rows"));
						m = p.matcher(matchhtml);

						while (m.find()) {
							// parse found rows
							JsonObject jobject = new JsonObject();
							jobject.add("table", key);
							jobject.add("offenderhash", this.offenderhash);

							String jstring;
							// add the header
							// force error on purpose --> unchecked
							// ArrayIndexOutOfBounds and NullPointerException
							if (header != null) {

								jstring = ((m.group().contains(header) == false) ? header
										+ "\n" + m.group()
										: m.group());

							} else {
								jstring = m.group();
							}

							if (footer != null && jstring != null) {
								jstring += footer;
							}
							jsoup = Jsoup.parse(jstring);

							// add attribute-value mapping for each record
							for (String innerkey : xpaths.get(key).keySet()) {

								if (innerkey.compareTo("narrow") != 0
										&& innerkey.compareTo("rows") != 0) {
									String[] atvals = xpaths.get(key)
											.get(innerkey).trim().split("=");
									Elements els = jsoup
											.getElementsByAttributeValue(
													atvals[0], atvals[1]);

									if (els.size() > 0) {
										jobject.add(
												innerkey.trim(),
												((this.replace != null) ? els
														.get(0)
														.text()
														.trim()
														.replaceAll(
																this.replace,
																this.replaceSequence)
														: els.get(0).text()
																.trim()));
									} else {
										jobject.add(innerkey.trim(), "");
									}
								}
							}

							// add json record to arraylist
							jsons.add(jobject.toString());
						}
					}
				}
			}
			jsoup = null;
			p = null;
			m = null;
			// return json records
			return jsons;
		}
	}

	/**
	 * Acts in the Same Way as the Regular Expression Version by Pipe Separating
	 * Values and inserting the result into a column.
	 * 
	 * @author aevans
	 *
	 */
	private class ParseLoop implements Callable<ArrayList<String>> {

		private final String header;
		private final String pagenarrow;
		private final Map<String, Map<String, String>> xpaths;
		private final String html;
		private final String replace;
		private final String replaceSequence;
		private final String offenderhash;
		private final String footer;

		ParseLoop(String offenderhash, String header, String footer,
				String pagenarrow, Map<String, Map<String, String>> xpaths,
				String html, String replace, String replaceSequence) {
			this.header = header;
			this.pagenarrow = pagenarrow;
			this.xpaths = xpaths;
			this.html = html;
			this.replace = replace;
			this.offenderhash = offenderhash;
			this.replaceSequence = replaceSequence;
			this.footer = footer;
		}

		@Override
		public ArrayList<String> call() throws Exception {
			// TODO return the Parsed XPath String with any narrowing effects.
			// Looped
			String matchhtml = this.html;
			ArrayList<String> jsons = new ArrayList<String>();
			Document jsoup;

			// narrow page if requested
			if (this.pagenarrow != null) {
				Pattern p = Pattern.compile(this.pagenarrow.trim());
				Matcher m = p.matcher(this.html);

				if (m.find()) {
					if (header != null) {
						matchhtml = (m.group().contains(header)) ? header
								+ "\n" + m.group() : m.group();
					} else {
						matchhtml = m.group();
					}

					if (footer != null) {
						matchhtml += footer;
					}
				}
			}

			// narrow

			// get the splittables
			// splittables are defined in Map<table,Map<key, value>
			for (String key : xpaths.keySet()) {

				if (xpaths.get(key).containsKey("narrow")) {
					Pattern p = Pattern.compile(this.pagenarrow.trim());
					Matcher m = p.matcher(this.html);

					if (m.find()) {
						matchhtml = (m.group().contains(header)) ? header
								+ "\n" + m.group() : m.group();

						if (footer != null) {
							matchhtml += footer;
						}
					}

				}

				jsoup = Jsoup.parse(matchhtml);
				JsonObject jobject = new JsonObject();
				jobject.add("table", key.trim());
				jobject.add("offenderhash", this.offenderhash.trim());
				String elements = null;
				String ikey = null;

				for (String innerkey : xpaths.get(key).keySet()) {

					if (ikey == null) {
						ikey = innerkey;
					}

					if (key.compareTo("narrow") != 0) {
						String[] attr = xpaths.get(key).get(innerkey)
								.split("=");
						Elements els = jsoup.getElementsByAttributeValue(
								attr[0].trim(), attr[1].trim());

						for (Element el : els) {
							elements = (elements == null) ? el.text()
									: elements + "|" + el.text();
						}
					}
				}
				jobject.add(
						ikey.trim(),
						((this.replace == null) ? elements.trim() : elements
								.trim().replaceAll(this.replace,
										this.replaceSequence)));
				jsons.add(jobject.toString());
			}
			jsoup = null;
			return jsons;
		}
	}

	// //GETTERS AND SETTERS///

	public String getHeader() {
		return header;
	}

	public String getCheckstring() {
		return checkstring;
	}

	public void setCheckstring(String checkstring) {
		this.checkstring = checkstring;
	}

	public int getMaxchecks() {
		return maxchecks;
	}

	public void setMaxchecks(int maxchecks) {
		this.maxchecks = maxchecks;
	}

	public String getNotnull() {
		return notnull;
	}

	public void setNotnull(String notnull) {
		this.notnull = notnull;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getPagenarrow() {
		return pagenarrow;
	}

	public void setPagenarrow(String pagenarrow) {
		this.pagenarrow = pagenarrow;
	}

	public String getTargetTable() {
		return targetTable;
	}

	@Required
	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getSelect() {
		return select;
	}

	@Required
	public void setSelect(String select) {
		this.select = select;
	}

	public String getReplace() {
		return replace;
	}

	public void setReplace(String replace) {
		this.replace = replace;
	}

	public String getReplaceSequence() {
		return replaceSequence;
	}

	public void setReplaceSequence(String replaceSequence) {
		this.replaceSequence = replaceSequence;
	}

	public String getExtracondition() {
		return extracondition;
	}

	public void setExtracondition(String extracondition) {
		this.extracondition = extracondition;
	}

	public int getQnums() {
		return qnums;
	}

	public void setQnums(int qnums) {
		this.qnums = qnums;
	}

	public int getProcs() {
		return procs;
	}

	public void setProcs(int procs) {
		this.procs = procs;
	}

	public Map<String, String> getSinglepaths() {
		return singlepaths;
	}

	public void setSinglepaths(Map<String, String> singlepaths) {
		this.singlepaths = singlepaths;
	}

	public Map<String, Map<String, String>> getMultipaths() {
		return multipaths;
	}

	public void setMultipaths(Map<String, Map<String, String>> multipaths) {
		this.multipaths = multipaths;
	}

	public Map<String, Map<String, String>> getRecordpaths() {
		return recordpaths;
	}

	public void setRecordpaths(Map<String, Map<String, String>> recordpaths) {
		this.recordpaths = recordpaths;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public String getPullid() {
		return pullid;
	}

	@Required
	public void setPullid(String pullid) {
		this.pullid = pullid;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
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

	public int getCommitsize() {
		return commitsize;
	}

	public void setCommitsize(int commitsize) {
		this.commitsize = commitsize;
	}

	public String getFooter() {
		return footer;
	}

	public void setFooter(String footer) {
		this.footer = footer;
	}

	// SUPPORT METHODS
	private void createTables() {
		// TODO Build Tables at Startup

		log.info("Creating Tables @ "
				+ Calendar.getInstance().getTime().toString());

		if (this.singlepaths != null) {
			// no internment of strings executed once
			if (this.singlepaths.containsKey("table")) {
				if (this.singlepaths.size() > 0) {
					StringBuilder builder = new StringBuilder();
					builder.append("CREATE TABLE "
							+ this.singlepaths.get("table")
							+ " (offenderhash text,");
					Iterator<String> it = this.singlepaths.keySet().iterator();

					while (it.hasNext()) {
						String key = it.next();
						if (!key.equalsIgnoreCase("table")) {
							builder.append(key + " text");

							if (it.hasNext()) {
								builder.append(",");
							}
						}
					}
					builder.append(")");

					template.execute(builder.toString());
				}
			}
			log.info("Finished Creating Tables @ "
					+ Calendar.getInstance().getTime().toString());
		}

		if (this.multipaths != null) {
			if (this.multipaths.size() > 0) {
				Iterator<String> it = this.multipaths.keySet().iterator();
				while (it.hasNext()) {
					String key = it.next();
					StringBuilder builder = new StringBuilder();
					builder.append("CREATE TABLE " + key
							+ " (offenderhash text,");

					Iterator<String> inneriterator = this.multipaths.get(key)
							.keySet().iterator();
					while (inneriterator.hasNext()) {
						String ik = inneriterator.next();
						builder.append(ik + " text");

						if (inneriterator.hasNext()) {
							builder.append(",");
						}

					}

					builder.append(")");
					template.execute(builder.toString());
				}
			}
		}

		if (this.recordpaths != null) {
			if (this.recordpaths.size() > 0) {
				for (String key : this.recordpaths.keySet()) {
					StringBuilder builder = new StringBuilder();
					builder.append("CREATE TABLE " + key
							+ " (offenderhash text,");

					int it = 0;
					for (String attr : this.recordpaths.get(key).keySet()) {
						it++;
						builder.append(attr + " text");

						if (it < this.recordpaths.get(key).keySet().size()) {
							builder.append(",");
						}
					}
					builder.append(")");
					template.execute(builder.toString());
				}
			}
		}

	}

	// //RUN THE PROGRAM////

	/**
	 * Runs the Program
	 */
	public void run() {
		int its = 0;
		
		this.select = Properties.getProperty(this.select);
		this.extracondition = Properties.getProperty(this.extracondition);
		this.column = Properties.getProperty(this.column);
		
		createTables();
		log.info("Starting Parse via JSoup @ "
				+ Calendar.getInstance().getTime().toString());

		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors() * procs);
		Set<Callable<ArrayList<String>>> collection;
		List<Future<ArrayList<String>>> futures;
		ArrayList<String> data = new ArrayList<String>((commitsize + 10));
		ArrayList<String> outdata = new ArrayList<String>(
				((commitsize + 10) * 3));
		int offenderhash = offset;

		boolean run = true;
		int iteration = 0;

		int currpos = 0;
		do {
			collection = new HashSet<Callable<ArrayList<String>>>(qnums);
			log.info("Getting Data");
			// get data
			currpos = iteration * commitsize + offset;
			iteration += 1;
			String query = select;

			if (extracondition != null) {
				query += " " + extracondition;
			}

			if (extracondition != null) {
				query += " WHERE " + extracondition + " AND ";
			} else {
				query += " WHERE ";
			}

			for (int i = 0; i < qnums; i++) {

				if (currpos + (Math.round(commitsize / qnums * (i + 1))) < currpos
						+ commitsize) {
					collection
							.add(new SplitQuery(
									(query
											+ pullid
											+ " >= "
											+ Integer.toString(currpos
													+ (Math.round(commitsize
															/ qnums * (i))))
											+ " AND " + pullid + " < " + Integer
											.toString(currpos
													+ (Math.round(commitsize
															/ qnums * (i + 1)))))));
				} else {
					collection.add(new SplitQuery((query
							+ pullid
							+ " >= "
							+ Integer.toString(currpos
									+ (Math.round(commitsize / qnums * (i))))
							+ " AND " + pullid + " < " + Integer
							.toString(currpos + commitsize))));
				}
			}

			if (collection.size() > 0) {

				futures = fjp.invokeAll(collection);

				int w = 0;

				while (fjp.isQuiescent() == false
						&& fjp.getActiveThreadCount() > 0) {
					w++;
				}

				for (Future<ArrayList<String>> f : futures) {
					try {
						// TODO Get Pages to Parse
						data.addAll(f.get());
					} catch (NullPointerException e) {
						log.info("Some Data Returned Null");
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}

			collection = new HashSet<Callable<ArrayList<String>>>(data.size());
			// checkstring
			if (data.size() == 0 && checkstring != null && its <= maxchecks) {
				its++;
				collection.add(new SplitQuery(checkstring));

				futures = fjp.invokeAll(collection);

				int w = 0;
				while (fjp.isQuiescent() == false
						&& fjp.getActiveThreadCount() > 0) {
					w++;
				}

				for (Future<ArrayList<String>> f : futures) {
					try {
						// TODO Get Pages to Parse
						data.addAll(f.get());
					} catch (NullPointerException e) {
						log.info("Some Data Returned Null");
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}

			if (data.size() == 0) {
				// set to stop if size is0
				log.info("No Pages to Parse. Will Terminate");
				run = false;
			} else {
				// parse
				log.info("Starting JSoup Parse @ "
						+ Calendar.getInstance().getTime().toString());
				for (String json : data) {
					// faster json reader is minimal json but faster parser is
					// Simple Json
					Map<String, Json> jMap = Json.read(json).asJsonMap();

					if (jMap.containsKey("offenderhash")) {
						// string to int in case it is a string and has some
						// extra space
						offenderhash = Integer.parseInt(jMap
								.get("offenderhash").asString().trim());
					}

					boolean allow = true;

					if (mustcontain != null) {
						if (jMap.get(column).asString().contains(mustcontain) == false) {
							allow = false;
						}
					}

					if (cannotcontain != null) {
						if (jMap.get(column).asString().contains(cannotcontain)) {
							allow = false;
						}
					}

					// this is the fastest way. I was learning before and will
					// rewrite when time permits.
					if (allow == true) {
						if (jMap.containsKey("offenderhash")) {
							if (this.singlepaths != null) {
								collection.add(new ParseSingle(Integer
										.toString(offenderhash), header,
										footer, pagenarrow, singlepaths,
										StringEscapeUtils.unescapeXml(jMap.get(
												column).asString()), replace,
										replaceSequence));
							}

							if (this.multipaths != null) {
								collection.add(new ParseRows(Integer
										.toString(offenderhash), header,
										footer, pagenarrow, multipaths,
										StringEscapeUtils.unescapeXml(jMap.get(
												column).asString()), replace,
										replaceSequence));
							}

							if (this.recordpaths != null) {
								collection.add(new ParseLoop(Integer
										.toString(offenderhash), header,
										footer, pagenarrow, recordpaths,
										StringEscapeUtils.unescapeXml(jMap.get(
												column).asString()), replace,
										replaceSequence));
							}
						}
					}
					offenderhash += 1;

				}

				// complete parse
				log.info("Waiting for Parsing to Complete.");
				if (collection.size() > 0) {
					futures = fjp.invokeAll(collection);

					int w = 0;
					while (fjp.isQuiescent() && fjp.getActiveThreadCount() > 0) {
						w++;
					}

					log.info("Waited for " + Integer.toString(w) + " Cycles!");
					for (Future<ArrayList<String>> f : futures) {
						try {
							outdata.addAll(f.get());
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ExecutionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}
				log.info("Finished Parsing @ "
						+ Calendar.getInstance().getTime().toString());

				int cp = 0;
				// post data
				log.info("Posting Data @ "
						+ Calendar.getInstance().getTime().toString());
				if (outdata.size() > 0) {

					for (int i = 0; i < qnums; i++) {

						ArrayList<String> od = new ArrayList<String>(
								((cp + (Math.round(outdata.size() / qnums) - cp))));

						if (cp + (Math.round(outdata.size() / qnums)) < outdata
								.size()) {
							od.addAll(outdata.subList(cp,
									(cp + (Math.round(outdata.size() / qnums)))));
						} else {
							od.addAll(outdata.subList(cp, (outdata.size() - 1)));
						}
						fjp.execute(new SplitPost(template, od));
						cp += Math.round(outdata.size() / qnums);
					}

					int w = 0;
					while (fjp.getActiveThreadCount() > 0
							&& fjp.isQuiescent() == false) {
						w++;
					}
					log.info("Waited for " + Integer.toString(w) + " cycles!");

				}
				log.info("Finished Posting to DB @ "
						+ Calendar.getInstance().getTime().toString());

				// size should remain same with 10 slot buffer room
				data.clear();
				outdata.clear();
			}

			// my favorite really desperate attempt to actually invoke garbage
			// collection because of MASSIVE STRINGS
			System.gc();
			Runtime.getRuntime().gc();

		} while (run);

		log.info("Shutting Down FJP");
		// shutdown fjp
		if (fjp.isShutdown() == false) {
			fjp.shutdownNow();
		}

		log.info("Finished Parsing @ "
				+ Calendar.getInstance().getTime().toString());

	}
}
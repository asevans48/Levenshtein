package com.hygenics.parser;

/**
 * Parses data into key value pairs based on
 * Regex or Soup Tags. This is meant to work
 * in the same manner as the CrawlModules for Python.
 * ParseDispatcher will take in Soup tags and allow
 * manual mapping of keys to values. 
 * 
 * @author aevans
 */

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

import com.eclipsesource.json.JsonObject;

public class KVParser {
	private final Logger log = LoggerFactory.getLogger(MainApp.class);

	private boolean test = false;

	private long termtime = 1000000L;
	private String htmlColumn;
	private String hashColumn;
	private int qnums = 1;
	private int commitsize = 100;
	private int procs = 2;
	private String notnull;
	private String select;
	private String extracondition;
	private String pullid;
	private String checkString;
	private int checkIterations;

	private getDAOTemplate template;
	private int offset;
	private Map<String, Map<String, Map<String, Object>>> tags;
	private String replacePattern;
	private String replacement;

	public boolean isTest() {
		return test;
	}

	public void setTest(boolean test) {
		this.test = test;
	}

	public long getTermtime() {
		return termtime;
	}

	public void setTermtime(long termtime) {
		this.termtime = termtime;
	}

	public String getHtmlColumn() {
		return htmlColumn;
	}

	public void setHtmlColumn(String htmlColumn) {
		this.htmlColumn = htmlColumn;
	}

	public String getHashColumn() {
		return hashColumn;
	}

	public void setHashColumn(String hashColumn) {
		this.hashColumn = hashColumn;
	}

	public String getPullid() {
		return pullid;
	}

	public void setPullid(String pullid) {
		this.pullid = pullid;
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

	public int getCommitsize() {
		return commitsize;
	}

	public void setCommitsize(int commitsize) {
		this.commitsize = commitsize;
	}

	public int getProcs() {
		return procs;
	}

	public void setProcs(int procs) {
		this.procs = procs;
	}

	public int getCheckIterations() {
		return checkIterations;
	}

	public void setCheckIterations(int checkIterations) {
		this.checkIterations = checkIterations;
	}

	public String getCheckString() {
		return checkString;
	}

	public void setCheckString(String checkString) {
		this.checkString = checkString;
	}

	public String getNotnull() {
		return notnull;
	}

	public void setNotnull(String notnull) {
		this.notnull = notnull;
	}

	public String getSelect() {
		return select;
	}

	@Required
	public void setSelect(String select) {
		this.select = select;
	}

	public Map<String, Map<String, Map<String, Object>>> getTags() {
		return tags;
	}

	public void setTags(Map<String, Map<String, Map<String, Object>>> tags) {
		this.tags = tags;
	}

	public String getReplacement() {
		return replacement;
	}

	public void setReplacement(String replacement) {
		this.replacement = replacement;
	}

	public String getReplacePattern() {
		return replacePattern;
	}

	public void setReplacePattern(String replacePattern) {
		this.replacePattern = replacePattern;
	}

	@Autowired
	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public int getOffset() {
		return offset;
	}

	@Required
	public void setOffset(int offset) {
		this.offset = offset;
	}

	private void sendToDb(ArrayList<String> json, boolean split) {
		if (json.size() > 0)
			log.info("Records to Add: " + json.size());

		if (split) {

			ForkJoinPool f2 = new ForkJoinPool((Runtime.getRuntime()
					.availableProcessors() + ((int) Math.ceil(procs * qnums))));
			ArrayList<String> l;
			int size = (int) Math.ceil(json.size() / qnums);
			for (int conn = 0; conn < qnums; conn++) {
				l = new ArrayList<String>();
				if (((conn + 1) * size) < json.size()) {
					l.addAll(json.subList((conn * size), ((conn + 1) * size)));

				} else {

					l.addAll(json.subList((conn * size), json.size()));
				}
				f2.execute(new SplitPost(template, l));
			}
			int w = 0;
			while (f2.isQuiescent() == false && f2.getActiveThreadCount() > 0) {
				w++;
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

	/**
	 * A basic KV Parser that works directly on data.
	 * 
	 * @author aevans
	 *
	 */
	private class Parser implements Callable<ArrayList<String>> {
		private final Map<String, Map<String, Object>> tags;
		private final String replacePattern;
		private final String html;
		private final String replacement;
		private final String hash;
		private final String hashName;
		private final String table;

		public Parser(Map<String, Map<String, Object>> tags, String html,
				String replacePattern, String replacement, String hash,
				String hashName, String table) {
			this.tags = tags;
			this.html = StringEscapeUtils.unescapeHtml3(StringEscapeUtils
					.unescapeHtml4(StringEscapeUtils.unescapeXml(html)));
			this.replacePattern = replacePattern;
			this.replacement = replacement;
			this.hash = hash;
			this.hashName = hashName;
			this.table = table;
		}

		@Override
		public ArrayList<String> call() {
			// get data and write to outdata array
			ArrayList<String> outData = new ArrayList<String>();
			JsonObject sjson = new JsonObject();
			JsonObject gjson = new JsonObject();

			if (test) {
				log.info(this.html);
			}

			for (String tag : tags.keySet()) {
				String utag = tag.replaceAll(":\\|:\\d+", "");
				String type = (String) this.tags.get(tag).get("type");

				if (type.contains("soup")) {

					Document doc = Jsoup.parse(this.html);
					Elements elements = doc.getElementsByTag(utag);
					String attr = null;
					String text = null;

					if (this.tags.keySet().contains("text")) {
						// get text regex
						text = (String) this.tags.get(tag).get("text");
					}

					if (this.tags.keySet().contains("attr")) {
						// get attribute regex
						attr = (String) this.tags.get(tag).get("attr");
					}

					if (type.contains("group")) {
						int pos = 1;
						int kpos = (Integer) this.tags.get(tag).get("keyPos");
						int vpos = (Integer) this.tags.get(tag).get("valPos");
						int maxEl = 0;
						String keyAttr = (String) this.tags.get(tag).get(
								"keyAttr");
						String valueAttr = (String) this.tags.get(tag).get(
								"valAttr");

						if (this.tags.get(tag).containsKey("max")) {
							maxEl = (Integer) this.tags.get(tag).get("max");
						} else {
							maxEl = 2;
						}

						String key = null;
						String value = null;

						if (keyAttr != null && valueAttr != null) {
							for (Element element : elements) {
								boolean tagMatch = true;

								if (text != null && element.hasText()) {
									Pattern p = Pattern.compile(text);
									Matcher m = p.matcher(element.text());
									tagMatch = m.find();
								} else if (text != null && !element.hasText()) {
									tagMatch = false;
								}

								if (attr != null) {
									String[] attrArr = attr.split(":\\|:");
									if (element.hasAttr(attrArr[0].trim())) {
										Pattern p = Pattern.compile(attr);
										Matcher m = p.matcher(element
												.attr(attrArr[0].trim()));
										tagMatch = !m.find();
									} else {
										tagMatch = false;
									}
								}
								int el = 0;
								if (tagMatch) {
									if (pos == kpos) {
										if (keyAttr.equals("text")
												&& element.hasText()) {
											key = element.attr(keyAttr);
											pos += 1;
										} else if (keyAttr.equals("attr")
												&& element.hasAttr(keyAttr)) {
											key = element.attr(keyAttr);
											pos += 1;
										}

										if (kpos > vpos && key != null
												&& value != null) {
											key = key.replaceAll("\\s+", "");

											if (key.length() < 31
													&& key.length() > 0) {
												gjson.add(key, value);
											}

											el++;
											if (el == maxEl) {
												gjson.add("table", this.table);
												gjson.add(this.hashName,
														this.hash);
												gjson.add("date", Calendar
														.getInstance()
														.getTime().toString());
												outData.add(gjson.toString());
												gjson = new JsonObject();
												el = 0;
											}
											pos = 0;
											key = null;
											value = null;
										}
										pos += 1;

									} else if (pos == vpos) {
										if (valueAttr.equals("text")
												&& element.hasText()) {
											key = element.attr(valueAttr)
													.replaceAll("[^A-Za-z0-9]",
															"");
											pos += 1;
										} else if (valueAttr.equals("attr")
												&& element.hasAttr(valueAttr)) {
											key = element.attr(valueAttr)
													.replaceAll("[^A-Za-z0-9]",
															"");
											pos += 1;
										}

										if (kpos > vpos && key != null
												&& value != null) {
											key = key.replaceAll("\\s+", "");

											if (key.length() < 31
													&& key.length() > 0) {
												try {
													gjson.remove(key);
												} catch (Exception e) {

												}
												gjson.add(key.toLowerCase(),
														value);
											}

											el++;
											if (el == maxEl) {
												gjson.add("table", this.table);
												gjson.add(this.hashName,
														this.hash);
												gjson.add("date", Calendar
														.getInstance()
														.getTime().toString());
												outData.add(gjson.toString());
												gjson = new JsonObject();
												el = 0;
											}

											pos = 0;
											key = null;
											value = null;
										}
									}
								}
							}
						}

					} else {

						String keyAttr = (String) this.tags.get(tag).get(
								"keyAttr");
						String valAttr = (String) this.tags.get(tag).get(
								"valAttr");

						for (Element element : elements) {
							boolean tagMatch = true;
							if (element.hasAttr(keyAttr)
									&& ((valAttr.equals("attr") && element
											.hasAttr(valAttr)) || element
											.hasText())) {
								if (text != null && element.hasText()) {
									Pattern p = Pattern.compile(text);
									Matcher m = p.matcher(html);
									tagMatch = m.find();

								} else if (text != null && !element.hasText()) {
									tagMatch = false;
								}

								if (attr != null) {
									String[] attrArr = attr.split(":\\|:");

									if (element.hasAttr(attrArr[0])) {
										Pattern p = Pattern.compile(attrArr[0]);
										Matcher m = p.matcher(html);
										tagMatch = m.find();
									} else {
										tagMatch = false;
									}
								}

								if (tagMatch) {
									String key = null;
									String value = null;

									if (keyAttr.equals("text")) {
										key = element.text().replaceAll(
												"[^A-Za-z0-9]", "");
									} else {
										key = element.attr(keyAttr).replaceAll(
												"[^A-Za-z0-9]", "");
									}

									if (valAttr.equals("text")) {
										value = element.text().replaceAll(
												replacePattern, replacement);
									} else {
										value = element.attr(valAttr)
												.replaceAll(replacePattern,
														replacement);
									}

									if (key != null && value != null) {
										key = key.replaceAll("\\s+", "");

										if (key.length() < 31
												&& key.length() > 0) {

											try {
												sjson.remove(key);
											} catch (Exception e) {

											}

											sjson.add(key.toLowerCase(), value);
										}

										key = null;
										value = null;
									}
								}
							}
						}
					}

				} else {

					if (type.contains("group")) {
						int pos = 1;
						int kpos = (Integer) this.tags.get(tag).get("keyPos");
						int vpos = (Integer) this.tags.get(tag).get("valPos");
						int maxEl = 0;

						if (this.tags.get(tag).containsKey("max")) {
							maxEl = (Integer) this.tags.get(tag).get("max");
						} else {
							maxEl = 2;
						}

						int el = 0;

						Pattern p = Pattern.compile(tag);
						Matcher m = p.matcher(this.html);
						String key = null;
						String value = null;
						while (m.find()) {
							if (pos == kpos) {
								key = m.group(0).replaceAll("[^A-Za-z0-9]", "");
								key = key.replaceAll("\\s+", "").toLowerCase();

								if (kpos > vpos && key != null && value != null) {
									if (key.length() < 31) {
										gjson.add(key, value);
									}
									el++;
									if (el == maxEl) {
										gjson.add(this.hashName, this.hash);
										gjson.add("table", this.table);
										gjson.add("date", Calendar
												.getInstance().getTime()
												.toString());
										outData.add(gjson.toString());
										el = 0;
									}
									key = null;
									value = null;
									pos = 0;

								}
							} else if (pos == vpos) {
								value = m.group(0).replaceAll(replacePattern,
										replacement);

								if (vpos > kpos && key != null && value != null) {

									if (key.length() < 31 && key.length() > 0) {
										try {
											gjson.remove(key);
										} catch (Exception e) {

										}
										gjson.add(key.replaceAll("\\s+", ""),
												value);
									}

									if (el == maxEl) {
										gjson.add(this.hashName, this.hash);
										gjson.add("table", this.table);
										gjson.add("date", Calendar
												.getInstance().getTime()
												.toString());
										outData.add(gjson.toString());
										el = 0;
									}
									key = null;
									value = null;
									pos = 0;
								}

							}
							pos += 1;
						}
					} else {
						int kpos = (Integer) this.tags.get(tag).get("keyPos");
						int vpos = (Integer) this.tags.get(tag).get("valPos");
						Pattern p = Pattern.compile(tag);
						Matcher m = p.matcher(this.html);
						while (m.find()) {

							if (m.groupCount() >= Math.max(kpos, vpos)) {
								String key = m.group(kpos).replaceAll(
										"[^A-Za-z0-9]", "");
								key = key.replaceAll("\\s+", "").toLowerCase();
								if (key != null && key.length() < 31
										&& key.length() > 0) {
									try {
										sjson.remove(key);
									} catch (Exception e) {

									}
									sjson.add(
											key,
											m.group(vpos)
													.replaceAll(replacePattern,
															replacement));
								}
								key = null;
							}
						}
					}
				}
			}

			if (sjson.size() > 0) {
				sjson.add(this.hashName, this.hash);
				sjson.add("table", this.table);
				sjson.add("date", Calendar.getInstance().getTime().toString());
				outData.add(sjson.toString());
			}
			sjson = null;
			gjson = null;

			return outData;
		}
	}

	/***
	 * Post Data to the Database Across Multiple Threads. BE CAREFUL.
	 * 
	 * @author aevans
	 *
	 */
	private class SplitPost implements Runnable {

		private static final long serialVersionUID = 5942536165467154211L;
		private final getDAOTemplate template;
		private ArrayList<String> json;

		public SplitPost(final getDAOTemplate template,
				final ArrayList<String> json) {
			this.json = json;
			this.template = template;
		}

		@Override
		public void run() {
			HashMap<String, HashSet<String>> cols = new HashMap<String, HashSet<String>>();

			if (this.json != null) {
				// add keys to the table
				for (String str : this.json) {
					Map<String, Json> jmap = Json.read(str).asJsonMap();
					if (jmap.containsKey("table")) {
						String tk = jmap.get("table").asString();
						if (!cols.containsKey(tk)) {
							cols.put(tk, new HashSet<String>());
						}

						for (String k : jmap.keySet()) {
							if (!k.equals("table")) {
								cols.get(tk).add(k);
							}
						}
					}
				}

				// fix the json strings to have all possible columns
				HashMap<String, ArrayList<String>> sqls = new HashMap<String, ArrayList<String>>();
				for (String str : this.json) {
					Map<String, Json> jmap = Json.read(str).asJsonMap();
					if (jmap.containsKey("table")) {

						JsonObject json = new JsonObject();
						for (String col : cols
								.get(jmap.get("table").asString())) {
							if (!col.equals("table")) {
								json.add(col, " ");
							}
						}

						for (String k : jmap.keySet()) {
							if (!k.equals("table")) {
								json.set(k, jmap.get(k).asString());
							}
						}

						if (json.size() > 0) {
							if (!sqls.containsKey(jmap.get("table").asString())) {
								sqls.put(jmap.get("table").asString(),
										new ArrayList<String>());
							}

							sqls.get(jmap.get("table").asString()).add(
									json.toString());
						}

					}
				}

				// post what has been found
				for (String k : sqls.keySet()) {
					log.info("Posting Data");
					// send remaining strings to db
					if (sqls.get(k).size() > 0) {
						log.info("Posting");
						String sql = "INSERT INTO " + k;
						String vals = "";
						String qend = "";

						for (String ck : Json.read(sqls.get(k).get(0))
								.asJsonMap().keySet()) {
							if (!ck.equals("table")) {
								vals += (vals.trim().length() == 0) ? "(" + ck
										: "," + ck;
								qend += (qend.trim().length() == 0) ? " VALUES(?"
										: ",?";
							}
						}
						sql += vals + ") " + qend + ")";
						this.template.postJsonData(sql, sqls.get(k));
					}
				}
				sqls = null;
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
			return template.getJSonDatawithQuotes(select);
		}
	}

	/**
	 * Check JSon Arrays for Tables
	 * 
	 * @param Json
	 */
	public void checkTables(ArrayList<String> jsonArr) {
		log.info("Checking Tables");
		HashMap<String, ArrayList<String>> tableMap = new HashMap<String, ArrayList<String>>(
				jsonArr.size());

		for (String json : jsonArr) {
			Map<String, Json> jmap = Json.read(json).asJsonMap();
			if (jmap.containsKey("table")) {
				String tk = jmap.get("table").asString();
				if (!tableMap.containsKey(tk)) {
					tableMap.put(tk, new ArrayList<String>());
				}

				for (String ck : Json.read(json).asMap().keySet()) {
					if (!ck.equals("table")) {
						if (!tableMap.get(tk).contains(ck.toLowerCase())) {
							tableMap.get(tk).add(ck.toLowerCase());
						}
					}
				}
			}
		}

		for (String table : tableMap.keySet()) {
			String[] tableArr = table.split("\\.");
			if (template.checkTable(table, tableArr[0]) == false) {
				String sql = "CREATE TABLE IF NOT EXISTS " + table + "(";
				int i = 0;
				for (String k : tableMap.get(table)) {
					if (!k.equals("table")) {
						sql += (i == 0) ? k + " text" : "," + k + " text";
						i++;
					}
				}
				sql += ")";
				template.execute(sql);
			} else {
				for (String k : tableMap.get(table)) {
					if (!k.equals("table") && !template.columnExists(table, k)
							&& !template.columnExists(table, k.toLowerCase())) {
						String sql = "ALTER TABLE " + table + " ADD COLUMN "
								+ k + " text";
						template.execute(sql);
					}
				}
			}

		}

	}

	public void run() {
		log.info("Starting Parse @ "
				+ Calendar.getInstance().getTime().toString());
		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors() * procs);
		Set<Callable<ArrayList<String>>> collection;
		List<Future<ArrayList<String>>> futures;
		ArrayList<String> data = new ArrayList<String>((commitsize + 10));
		ArrayList<String> outdata = new ArrayList<String>(
				((commitsize + 10) * 3));

		int currpos = 0;
		boolean run = true;

		while (run) {
			log.info("Getting Pages");
			// get pages
			String query = select;

			if (data.size() > 0) {
				data.clear();
			}

			if (extracondition != null) {
				query += " " + extracondition;
			}

			if (extracondition != null) {
				query += " WHERE " + extracondition + " AND ";
			} else {
				query += " WHERE ";
			}

			collection = new HashSet<Callable<ArrayList<String>>>(qnums);
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

			currpos += commitsize;

			if (collection.size() > 0) {

				futures = fjp.invokeAll(collection);

				int w = 0;

				while (fjp.isQuiescent() == false
						&& fjp.getActiveThreadCount() > 0) {
					w++;
				}

				for (Future<ArrayList<String>> f : futures) {
					try {
						ArrayList<String> darr = f.get();
						if (darr != null && darr.size() > 0) {
							data.addAll(darr);
						}
					} catch (NullPointerException e) {
						log.info("Some Data Returned Null");
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}

			}

			if (data.size() == 0 && checkString != null) {
				collection = new HashSet<Callable<ArrayList<String>>>(1);
				collection.add(new SplitQuery(checkString));

				futures = fjp.invokeAll(collection);
				int w = 0;
				while (fjp.isQuiescent() == false
						&& fjp.getActiveThreadCount() > 0) {
					w++;
				}

				for (Future<ArrayList<String>> f : futures) {
					try {
						ArrayList<String> arr = f.get();

						if (arr != null) {
							for (String a : arr) {
								if (a != null) {
									data.add(a);
								}
							}
						}
						if (!f.isDone()) {
							f.cancel(true);
						}
						f = null;
					} catch (NullPointerException e) {
						log.info("Some Data Returned Null");
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
			}

			// parse pages
			if (data.size() > 0) {
				log.info("Parsing " + Integer.toString(data.size())
						+ " Records");
				collection = new HashSet<Callable<ArrayList<String>>>(
						data.size());

				for (String json : data) {
					Map<String, Object> jmap = Json.read(json).asMap();

					// for each table in the tags Map which is a key
					for (String k : tags.keySet()) {

						collection.add(new Parser(tags.get(k), jmap.get(
								htmlColumn).toString(), replacePattern,
								replacement, jmap.get(hashColumn).toString(),
								hashColumn, k));

						if (collection.size() + 1 == data.size()
								|| (collection.size() % commitsize == 0 && collection
										.size() >= commitsize)) {
							log.info("Waiting for Tasks to Complete");
							futures = fjp.invokeAll(collection);

							// post data
							int w = 0;
							while (fjp.isQuiescent() == false
									&& fjp.getActiveThreadCount() > 0) {
								w++;
							}

							for (Future<ArrayList<String>> future : futures) {
								try {
									outdata.addAll(future.get());
								} catch (NullPointerException e) {
									log.info("Some Data Returned Null");
								} catch (InterruptedException e) {
									e.printStackTrace();
								} catch (ExecutionException e) {
									e.printStackTrace();
								}
							}

							log.info("Parsed " + outdata.size() + " records!");
							// post data
							int cp = 0;
							if (outdata.size() > 0) {
								checkTables(outdata);
								this.sendToDb(outdata, true);
								outdata = new ArrayList<String>(commitsize);
							}

						}

					}
				}
				data = new ArrayList<String>(commitsize);
			} else {
				log.info("No Records Found. Terminating!");
				run = false;
			}

		}

		if (outdata.size() > 0) {
			log.info("Posting Last Records");
			// post remaining pages for the iteration
			if (outdata.size() > 0) {
				int cp = 0;
				if (outdata.size() > 0) {
					checkTables(outdata);
					this.sendToDb(outdata, true);
				}
				data.clear();
				outdata.clear();
			}
		}

		// shutdown
		log.info("Complete! Shutting Down FJP.");
		fjp.shutdownNow();

		log.info("Finished Parse @ "
				+ Calendar.getInstance().getTime().toString());
	}

}
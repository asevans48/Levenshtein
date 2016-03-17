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
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

import mjson.Json;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

import com.hygenics.scala.RegexAPI;

import com.eclipsesource.json.JsonObject;
import com.hygenics.jdbc.jdbcconn;

public class ParseDispatcher {

	private int reattempts = 10;

	private Map<String, String> index;
	private boolean escape = false;
	private boolean unescape = false;
	private boolean cascade = true;
	private boolean truncate = true;
	private boolean test = false;
	private String key;
	private String fkey;
	private String fkeyref;

	private String extracondition;
	private String notnull;
	private int pullsize = 100;
	private int loops = 0;
	private int waitloops = 1;

	private int SPLITSIZE = 50;
	private Logger log = LoggerFactory.getLogger(MainApp.class);

	private int termtime = 500;
	private int qnum = 5;
	private jdbcconn jdbc;
	private int sqlnum = 1;

	private String schema;
	private int procnum = 2;

	private String cannotcontain;
	private String mustcontain;
	private String replacementPattern;
	private String pullid;
	private String select;
	private String column;
	private String post;

	private String checkstring;

	private int commit_size = 100;

	private boolean getHash = true;

	private String imageRegex;
	private String imageprefix;
	private String imagesuffix;

	private ArrayList<String> pages = new ArrayList<String>();

	// these shouldn't be too large
	private Map<String, String> singlepats = new HashMap<String, String>();
	private Map<String, Map<String, String>> multipats = new HashMap<String, Map<String, String>>();
	private Map<String, Map<String, String>> loopedpats = new HashMap<String, Map<String, String>>();

	@Autowired
	private getDAOTemplate template;
	private int offset;

	ParseDispatcher() {

	}

	public void setIndex(Map<String, String> index) {
		this.index = index;
	}

	public Map<String, String> getIndex() {
		return this.index;
	}

	public int getReattempts() {
		return reattempts;
	}

	public void setReattempts(int reattempts) {
		this.reattempts = reattempts;
	}

	public String getCheckstring() {
		return checkstring;
	}

	public void setCheckstring(String checkstring) {
		this.checkstring = checkstring;
	}

	public String getFkey() {
		return fkey;
	}

	public void setFkey(String fkey) {
		this.fkey = fkey;
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

	public String getExtracondition() {
		return extracondition;
	}

	public void setExtracondition(String extracondition) {
		this.extracondition = extracondition;
	}

	public String getCannotcontain() {
		return cannotcontain;
	}

	public void setCannotcontain(String cannotcontain) {
		this.cannotcontain = cannotcontain;
	}

	public String getMustcontain() {
		return mustcontain;
	}

	public void setMustcontain(String mustcontain) {
		this.mustcontain = mustcontain;
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

	public void setSPLITSIZE(int SPLITSIZE) {
		this.SPLITSIZE = SPLITSIZE;
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

	public String getNotnull() {
		return notnull;
	}

	public void setNotnull(String notnull) {
		this.notnull = notnull;
	}

	public int getPullsize() {
		return pullsize;
	}

	public void setPullsize(int pullsize) {
		this.pullsize = pullsize;
	}

	public Map<String, Map<String, String>> getLoopedpats() {
		return loopedpats;
	}

	public void setLoopedpats(Map<String, Map<String, String>> loopedpats) {
		this.loopedpats = loopedpats;
	}

	public ArrayList<String> getPages() {
		return pages;
	}

	public void setPages(ArrayList<String> pages) {
		this.pages = pages;
	}

	public String getSchema() {
		return schema;
	}

	@Required
	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getReplacementPattern() {
		return replacementPattern;
	}

	public void setReplacementPattern(String replacementPattern) {
		this.replacementPattern = replacementPattern;
	}

	public String getPost() {
		return post;
	}

	public void setPost(String post) {
		this.post = post;
	}

	public jdbcconn getJdbc() {
		return jdbc;
	}

	public void setJdbc(jdbcconn jdbc) {
		this.jdbc = jdbc;
	}

	public String getPullid() {
		return pullid;
	}

	@Required
	public void setPullid(String pullid) {
		this.pullid = pullid;
	}

	public String getSelect() {
		return select;
	}

	@Required
	public void setSelect(String select) {
		this.select = select;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public int getCommit_size() {
		return commit_size;
	}

	public void setCommit_size(int commit_size) {
		this.commit_size = commit_size;
	}

	public String getImageRegex() {
		return imageRegex;
	}

	public void setImageRegex(String imageRegex) {
		this.imageRegex = imageRegex;
	}

	public String getImageprefix() {
		return imageprefix;
	}

	public void setImageprefix(String imageprefix) {
		this.imageprefix = imageprefix;
	}

	public String getImagesuffix() {
		return imagesuffix;
	}

	public void setImagesuffix(String imagesuffix) {
		this.imagesuffix = imagesuffix;
	}

	public Map<String, String> getSinglepats() {
		return singlepats;
	}

	public void setSinglepats(Map<String, String> singlepats) {
		this.singlepats = singlepats;
	}

	public Map<String, Map<String, String>> getMultipats() {
		return multipats;
	}

	public void setMultipats(Map<String, Map<String, String>> multipats) {
		this.multipats = multipats;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	public int getOffset() {
		return offset;
	}

	@Required
	public void setOffset(int offset) {
		this.offset = offset;
	}

	public Logger getLog() {
		return log;
	}

	public boolean isUnescape() {
		return unescape;
	}

	public void setUnescape(boolean unescape) {
		this.unescape = unescape;
	}

	public boolean isEscape() {
		return escape;
	}

	public void setEscape(boolean escape) {
		this.escape = escape;
	}

	public boolean isCascade() {
		return cascade;
	}

	public void setCascade(boolean cascade) {
		this.cascade = cascade;
	}

	public boolean isTruncate() {
		return truncate;
	}

	public void setTruncate(boolean truncate) {
		this.truncate = truncate;
	}

	public boolean isGetHash() {
		return getHash;
	}

	public void setGetHash(boolean getHash) {
		this.getHash = getHash;
	}

	public boolean isTest() {
		return test;
	}

	public void setTest(boolean test) {
		this.test = test;
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
			// TODO Auto-generated method stub
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
							|| jmap.size() > numvals || jmap.size() < numvals) {
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

	private class SplitQuery implements Callable<ArrayList<String>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -8185035798235011678L;
		private final getDAOTemplate template;
		private String sql;

		private SplitQuery(final getDAOTemplate template, final String sql) {
			this.template = template;
			this.sql = sql;
		}

		@Override
		public ArrayList<String> call() {
			// TODO Auto-generated method stub
			ArrayList<String> results = template.getJsonData(sql);
			return results;
		}
	}

	/**
	 * Computes Regexes @ 100% CPU for Multiple Result Regexes and returns a
	 * json string with the result.
	 * 
	 * @author aevans
	 *
	 */
	private class ParseMultiPage implements Callable<String> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2269985121520139450L;
		private final boolean unescape;
		private String html;
		private String offenderhash;
		private String date;
		private Map<String, String> regexes;
		private String table;
		private String replacementPattern;

		public ParseMultiPage(final boolean unescape,
				final String replacementPattern, final String table,
				final String html, final String offenderhash,
				final String date, final Map<String, String> regexes) {
			super();
			this.unescape = unescape;
			this.replacementPattern = replacementPattern;
			this.table = table;
			this.html = html.replaceAll("\t|\r|\r\n|\n", "");
			this.offenderhash = offenderhash;
			this.date = date;
			this.regexes = regexes;
		}

		@Override
		public String call() {
			String json = null;
			String[] results;
			RegexAPI reg = new RegexAPI();
			String page = this.html;
			Set<String> set = this.regexes.keySet();

			if (this.unescape == true) {
				page = StringEscapeUtils.unescapeXml(StringEscapeUtils
						.unescapeHtml3(this.html));
			}

			for (String s : set) {
				reg.setPattern(regexes.get(s));
				results = reg.multi_regex(page);

				if (results != null) {
					for (int i = 0; i < results.length; i++) {
						json = (json == null) ? "{\"" + s + "\":\""
								+ results[i] + "\",\"table\":\"" + table
								+ "\",\"offenderhash\":\"" + offenderhash
								+ "\",\"date\":\"" + date + "\"}" : json + "~"
								+ "{\"" + s + "\":\"" + results[i]
								+ "\",\"table\":\"" + table
								+ "\",\"offenderhash\":\"" + offenderhash
								+ "\",\"date\":\"" + date + "\"}";
					}
				} else {
					json = "No Data";
				}
			}

			if (json != null) {

				if (this.replacementPattern != null) {
					json = StringEscapeUtils
							.unescapeHtml3(
									StringEscapeUtils
											.unescapeHtml4(StringEscapeUtils
													.unescapeXml(json)))
							.replaceAll(replacementPattern, " ").trim();
				}
			} else {
				json = "No Data";
			}

			html = null;
			regexes = null;
			page = null;
			this.html = null;
			return json;
		}

	}

	/**
	 * Computes Regexes @ 100% CPU for single result regexes and returns a json
	 * string with the result.
	 * 
	 * @author aevans
	 */
	private class ParsePage implements Callable<String> {
		private final boolean unescape;
		private static final long serialVersionUID = 2486584810895316538L;
		private final String html;
		private final String offenderhash;
		private final String date;
		private final String table;
		private final String replacementPattern;

		private Map<String, String> regex;

		public ParsePage(final boolean unescape,
				final String replacementPattern, final String table,
				final String inhtml, final Map<String, String> regex,
				final String date, final String offenderhash) {
			this.unescape = unescape;
			this.replacementPattern = replacementPattern;
			this.table = table;
			this.offenderhash = offenderhash;
			this.html = inhtml.replaceAll("\t|\r|\r\n|\n", "");
			this.regex = regex;
			this.date = date;
		}

		@Override
		public String call() {
			String page = this.html;
			if (this.unescape) {
				page = StringEscapeUtils.unescapeXml(StringEscapeUtils
						.unescapeHtml3(this.html));
			}

			String json = null;
			String result;
			RegexAPI reg = new RegexAPI();
			Set<String> set = this.regex.keySet();

			if (page != null) {
				Document p = Jsoup.parse(Jsoup.parse(page).html());
				// log.info(p.html());
				JsonObject jobj = new JsonObject();// jobj
				for (String r : set) {
					result = null;
					// setup
					if (r.contains("soup") == false) {
						reg.setPattern(this.regex.get(r));
						result = reg.single_regex(page);

						// add to jobj
						if (result != null && r.compareTo("table") != 0
								&& r.compareTo("offenderhash") != 0
								&& r.compareTo("date") != 0) {
							jobj.add(StringEscapeUtils
									.escapeXml(r.replaceAll(
											replacementPattern, " ").trim()),
									StringEscapeUtils
											.escapeXml(result.replaceAll(
													replacementPattern, " ")
													.trim()));
						} else if (r.compareTo("table") != 0
								&& r.compareTo("offenderhash") != 0
								&& r.compareTo("date") != 0) {
							jobj.add(
									StringEscapeUtils.escapeXml(r.replaceAll(
											replacementPattern, " ").trim()),
									StringEscapeUtils.escapeXml(""));
						}
					} else {
						List<Element> els = null;

						if (r.contains("Id")) {

							Element sel = p.getElementById(this.regex.get(r));

							if (sel != null) {
								jobj.add(r.replaceAll("soup.*", ""),
										StringEscapeUtils.escapeXml(sel
												.text()));
							} else {
								jobj.add(r.replaceAll("soup.*", ""), "");
							}
						} else if (r.contains("Tag")) {
							els = p.getElementsByTag(this.regex.get(r));
						} else if (r.contains("Class")) {
							els = p.getElementsByClass(this.regex.get(r));
						} else if (r.contains("Attr")) {
							String[] arr = this.regex.get(r).split(";");

							if (arr.length > 1) {
								String[] attrs = arr[0].split("=");
								if (attrs.length > 1) {
									els = p.getElementsByAttributeValue(
											attrs[0], attrs[1]);
									if (els.size() > 0) {
										Document ep2 = Jsoup
												.parseBodyFragment(els
														.toString());
										els = ep2
												.getElementsByAttribute(arr[1]);
										if (els.size() > 0) {
											jobj.add(r.replaceAll("soup.*", "")
													.trim(), StringEscapeUtils
													.escapeXml(els.get(0)
															.attr(arr[1])
															.trim()));
										}
									}
								}

								if (!jobj.names().contains(
										r.replaceAll("soup.*", "").trim())) {
									jobj.add(r.replaceAll("soup.*", "").trim(),
											"");
								}

								els = null;
							}

						} else if (r.contains("Value")) {

							String[] arr = this.regex.get(r).split("=");

							if (arr.length > 1) {
								els = p.getElementsByAttributeValue(arr[0],
										arr[1]);
							}
						} else {
							els = Jsoup.parse(page).select(this.regex.get(r));
						}

						if (els != null && r.contains("Id")) {
							jobj.add(r.replaceAll("soup.*", "").trim(),
									StringEscapeUtils.escapeXml(els.get(0)
											.text()));
						} else if (r.contains("Id") == false
								&& r.contains("Attr") == false) {
							jobj.add(r.replaceAll("soup.*", ""), "");
						}
					}
				}

				if (jobj != null && jobj.size() > 0) {
					// add final elements
					jobj.add("table", this.table).add("date", this.date)
							.add("offenderhash", this.offenderhash);
					json = jobj.toString();
					if (json.trim().length() == 0) {
						json = "No Data";
					}
				} else {
					json = "No Data";
				}

				regex = null;
			}

			return StringEscapeUtils.unescapeHtml3(StringEscapeUtils
					.unescapeHtml4(StringEscapeUtils.unescapeXml(json)));
		}
	}

	/**
	 * Generates a unique hash for each row
	 * 
	 * @author aevans
	 *
	 */
	private class CreateHash implements Callable<String> {
		private Logger Log = LoggerFactory.getLogger(MainApp.class);
		private final String json;
		private final int id;

		public CreateHash(String json, int id) {
			this.json = json;
			this.id = id;
		}

		public String genHash() {
			return (id + Long
					.toString(Calendar.getInstance().getTimeInMillis()));
		}

		@Override
		public String call() throws Exception {
			JsonObject jobject = JsonObject.readFrom(StringEscapeUtils
					.unescapeJava(json));

			try {

				jobject.remove("offenderhash");
				jobject.add("offenderhash", genHash());

				return jobject.toString();
			} catch (Exception e) {
				try {
					jobject.add("offenderhash", genHash());
					return jobject.toString();
				} catch (Exception e2) {
					log.warn("CANNOT HASH.");
					e2.printStackTrace();
					return null;
				}
			}
		}

	}

	/**
	 * This class is used to perform a "looping regex" where elements are broke
	 * down to a piece of the page and then further parsed. It is still a bit
	 * faster than getting every tag and then regexing for certain information.
	 * Really, I tested it
	 * 
	 * Comparison of operations: XML:toXML-->compile -->parse,
	 * -->compile-->parse-->check condition-->replace-->json v. REGEX:split
	 * reg-->compile--> parse-->compile-->parse-->replace-->json
	 * 
	 * 
	 * @author aevans
	 *
	 */
	private class LoopRegex implements Callable<String> {
		private final boolean unescape;
		private static final long serialVersionUID = 7604436478239646264L;
		private String html;
		private String offenderhash;
		private String date;
		private String table;
		private String replacementPattern;
		private Map<String, String> patterns;
		private boolean test;

		public LoopRegex(final boolean unescape, final String html,
				final String offenderhash, final String date,
				final String table, final String replacementPattern,
				final Map<String, String> patterns, final boolean test) {
			super();
			this.unescape = unescape;
			this.html = html.replaceAll("\t|\r|\r\n|\n", "");
			this.offenderhash = offenderhash;
			this.date = date;
			this.table = table;
			this.replacementPattern = replacementPattern;
			this.patterns = patterns;
			this.test = test;
		}

		@Override
		public String call() {
			String json = null;
			RegexAPI reg = new RegexAPI();
			Set<String> keys = patterns.keySet();
			String result = null;
			String[] results = null;
			Boolean mustcontain = true;
			String secondres = null;
			String stringified = null;
			Map<String, String> resultsmap = new HashMap<String, String>();

			String page = this.html;
			if (this.unescape) {
				page = StringEscapeUtils.unescapeXml(StringEscapeUtils
						.unescapeHtml3(page));
			}

			if (keys.contains("mustcontain")) {
				mustcontain = page.contains(patterns.get("mustcontain"));
			}

			if (mustcontain && (page != null || result != null)) {

				if (test == true && unescape == true) {
					System.out
							.println("//////////////////////HTML////////////////////////\n"
									+ StringEscapeUtils
											.unescapeXml(StringEscapeUtils
													.unescapeHtml3(page))
									+ "\n///////////////////////////////END///////////////////////////\n\n");
				} else if (test) {
					System.out
							.println("//////////////////////HTML////////////////////////\n"
									+ page
									+ "\n///////////////////////////////END///////////////////////////\n\n");
				}

				for (String k : keys) {

					reg.setPattern(patterns.get(k));

					if (k.contains("narrow")) {

						if (k.contains("additional")) {
							page = result;
						}

						if (k.contains("soup")) {
							Document doc = Jsoup.parse(page);
							List<Element> els = null;
							if (k.contains("Id")) {
								Element el = doc
										.getElementById(patterns.get(k));
								if (el != null) {
									result = el.toString();
								}
							} else if (k.contains("Tag")) {
								els = doc.getElementsByTag(patterns.get(k));

							} else if (k.contains("Class")) {
								els = doc.getElementsByClass(patterns.get(k));
							} else if (k.contains("Value")) {
								String[] arr = patterns.get(k).split("=");

								if (arr.length > 1) {
									els = doc.getElementsByAttributeValue(
											arr[0], arr[1]);
								}
							} else {
								els = doc.select(patterns.get(k));
							}

							if (els != null && els.size() > 0) {
								result = els.get(0).toString();
							}
						} else {
							result = reg.single_regex(page);
						}
					} else if (k.contains("soup") == false
							&& k.contains("PsingleP")) {
						if (result != null) {

							secondres = reg.single_regex(result);

							if (secondres != null) {

								resultsmap.put(k.replaceAll("PsingleP", ""),
										secondres.trim());
								secondres = null;
							}
						}
					} else if (k.contains("soup") == false
							&& k.contains("mustcontain") == false) {
						if (result != null) {

							results = reg.multi_regex(result.replaceAll(
									"\t|\r|\r\n", ""));

							if (results != null) {
								for (String s : results) {
									stringified = (stringified == null) ? s
											: stringified + "|" + s;
								}

								resultsmap.put(StringEscapeUtils.escapeJson(k),
										StringEscapeUtils
												.escapeJson(stringified));
								stringified = null;
								results = null;
							}

						}
					} else if (k.contains("soup") == true && result != null
							&& k.contains("mustcontain") == false) {
						if (result != null) {
							stringified = null;

							List<Element> els = null;
							if (k.contains("Id")) {
								Element el = Jsoup.parseBodyFragment(result)
										.getElementById(this.patterns.get(k));

								if (el != null) {
									stringified = (stringified == null) ? el
											.text() : stringified + "|"
											+ el.text();
								}
							} else if (k.contains("Tag")) {
								els = Jsoup.parseBodyFragment(result)
										.getElementsByTag(this.patterns.get(k));
							} else if (k.contains("Class")) {
								els = Jsoup.parseBodyFragment(result)
										.getElementsByClass(
												this.patterns.get(k));
							} else if (k.contains("Attr")) {
								String[] arr = this.patterns.get(k).split(";");

								if (arr.length > 1) {
									String[] attrs = arr[0].split("=");
									if (attrs.length > 1) {
										els = Jsoup.parseBodyFragment(result)
												.getElementsByAttributeValue(
														attrs[0], attrs[1]);
										if (els.size() > 0) {
											Document ep2 = Jsoup
													.parseBodyFragment(els
															.toString());
											els = ep2
													.getElementsByAttribute(arr[1]);
										}
									}
								}
							} else if (k.contains("Value")) {

								String[] arr = this.patterns.get(k).split("=");

								if (arr.length > 1) {
									els = Jsoup.parseBodyFragment(result)
											.getElementsByAttributeValue(
													arr[0], arr[1]);
								}
							} else {
								els = Jsoup.parseBodyFragment(result).select(
										patterns.get(k));
							}

							if (els != null) {
								for (Element el : els) {
									if (el.text() != null
											&& el.text().equals("null") == false) {
										stringified = (stringified == null) ? StringEscapeUtils
												.escapeJson(el.text()
														.replaceAll(
																"\t|\r|\r\n",
																""))
												: stringified
														+ "|"
														+ StringEscapeUtils
																.escapeJson(el
																		.text()
																		.replaceAll(
																				"\t|\r|\r\n",
																				""));
									}
								}

								if (stringified != null) {
									resultsmap.put(k.replaceAll("soup.*", ""),
											StringEscapeUtils
													.escapeXml(stringified));
								}
							}
						}
					}
				}

				if (resultsmap.size() > 0) {
					for (String k : resultsmap.keySet()) {
						json = (json == null) ? "{\""
								+ k
								+ "\":\""
								+ resultsmap.get(k).replaceAll(
										replacementPattern, " ") + "\"" : json
								+ ",\""
								+ k
								+ "\":\""
								+ resultsmap.get(k).replaceAll(
										replacementPattern, " ") + "\"";
					}
				}

				if (json == null) {
					json = "No Data";
				} else {
					json += ",\"table\":\"" + this.table + "\",\"date\":\""
							+ this.date + "\",\"offenderhash\":\""
							+ this.offenderhash + "\"}";
					json = json.trim();
				}

				this.patterns = null;
				page = null;
				this.html = null;
				stringified = null;
				secondres = null;
				resultsmap = null;

			} else {
				json = "No Data";
			}
			return json;
		}

	}

	private void getFromDb(String condition) {
		pages = template.getJsonData((select + condition));

	}

	private void spl(ArrayList<String> json, boolean split) {
		if (json.size() > 0)
			log.info("Records to Add: " + json.size());

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

	private void sendToDb(ArrayList<String> json, boolean split) {
		if (json.size() > 0)
			log.info("Records to Add: " + json.size());

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

	public String genHash(String id) {
		// currently just a bunch of random things to avoid problesm will
		// hopefully become attribute based in future
		return id + Long.toString(Calendar.getInstance().getTimeInMillis())
				+ Integer.toString((int) (Math.random() * 1000));
	}

	public void createTables() {
		Set<String> keys = null;
		String sql = null;
		String[] tablearr;
		String vals = null;

		log.info("Checking Tables");

		if (singlepats != null & schema != null) {
			log.info("Checking Singlepats Table");
			if (singlepats.containsKey("table")) {
				tablearr = singlepats.get("table").trim().split("\\.");
				if (this.template.checkTable(singlepats.get("table"),
						tablearr[0]) == false) {
					sql = "CREATE TABLE " + singlepats.get("table") + "(";

					keys = singlepats.keySet();

					if (keys.contains("id") == false) {
						if (key != null) {
							vals = "id SERIAL UNIQUE NOT NULL";
						} else {
							key = "";
							fkey = "";
							vals = "id SERIAL PRIMARY KEY NOT NULL";
						}
					}

					for (String k : keys) {
						if (k.compareTo("table") != 0) {
							if (k.compareTo(key) == 0) {
								vals += (vals == null) ? k
										+ " text PRIMARY KEY" : "," + k
										+ "text";
							} else if (k.compareTo(fkey) == 0) {
								vals += (vals == null) ? k
										+ " text FOREIGN KEY " + fkeyref : ","
										+ k + " text FOREIGN KEY " + fkeyref;
							} else {
								vals = (vals == null) ? k + " text" : vals
										+ ","
										+ k.replaceAll("(?mis)soup.*|psinglep",
												"") + " text";
							}
						}
					}

					sql += vals + ",date text,offenderhash text)";
					log.info("CREATING WITH: " + sql);
					this.template.execute(sql);
				} else if (truncate) {

					sql = "TRUNCATE " + singlepats.get("table");

					if (cascade) {
						sql += " CASCADE";
					}
					log.info("Truncating with: " + sql);
					template.execute(sql);
				}

			}

		}

		if (multipats != null & schema != null) {
			keys = multipats.keySet();
			log.info("Generating Tables for Multi-Pats");
			for (String table : keys) {
				if (this.template.checkTable(table, schema) == false) {
					log.info("Generating Table for " + table);
					this.template.createTable(table, schema,
							multipats.get(table).keySet(), key, fkey, fkeyref);
				} else if (truncate) {
					log.info("Truncating " + table);
					sql = "TRUNCATE " + table;

					if (cascade) {
						sql += " CASCADE";
					}
					log.info("Truncating with: " + sql);
					template.execute(sql);
				}
			}
		}

		if (loopedpats != null & schema != null) {

			keys = loopedpats.keySet();

			for (String table : keys) {
				if (this.template.checkTable(table, schema) == false) {
					this.template.createTable(table, schema,
							loopedpats.get(table).keySet(), key, fkey, fkeyref);
				} else if (truncate) {
					sql = "TRUNCATE " + table;

					if (cascade) {
						sql += " CASCADE";
					}
					log.info("Truncating with: " + sql);
					template.execute(sql);
				}
			}
		}

		// check for indices
		int i = 0;
		if (index != null) {
			for (String k : index.keySet()) {
				try {
					String sqlString = "CREATE INDEX " + this.schema
							+ Integer.toString(i) + " ON " + k + "("
							+ index.get(k) + ")";
					template.execute(sqlString);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Fork/Join Pool Solution Maximizes Speed. JSon increases ease of use
	 * 
	 */
	public void run() {
		log.info("Starting Clock and Parsing @"
				+ Calendar.getInstance().getTime().toString());
		long t = Calendar.getInstance().getTimeInMillis();
		int pid = 0;
		int id = 0;
		int checkattempts = 0;
		String add = null;
		
		this.schema = Properties.getProperty(this.schema);
		this.select = Properties.getProperty(this.select);
		this.extracondition = Properties.getProperty(this.extracondition);
		this.column = Properties.getProperty(this.column);
		
		ArrayList<String> parsedrows = new ArrayList<String>();

		Set<Callable<String>> collect = new HashSet<Callable<String>>();
		List<Future<String>> futures;

		List<Future<ArrayList<String>>> qfutures;
		Set<Callable<ArrayList<String>>> qcollect = new HashSet<Callable<ArrayList<String>>>(
				4);

		ForkJoinPool fjp = new ForkJoinPool((int) Math.ceil(Runtime
				.getRuntime().availableProcessors() * procnum));

		if (schema != null) {
			createTables();
		}

		boolean run = true;
		String condition;
		int w = 0;
		int start = offset;
		int chunksize = (int) Math.ceil(pullsize / qnum);

		// attempt to query the database from multiple threads
		do {
			// query for pages
			pages = new ArrayList<String>(pullsize);
			log.info("Looking for Pages.");
			for (int conn = 0; conn < qnum; conn++) {
				// create condition
				condition = " WHERE " + pullid + " >= "
						+ (start + (conn * chunksize)) + " AND " + pullid
						+ " < "
						+ Integer.toString(start + (chunksize * (conn + 1)));

				if (extracondition != null) {
					condition += " " + extracondition.trim();
				}

				// get queries
				qcollect.add(new SplitQuery(template, (select + condition)));
				log.info("Fetching " + select + condition);
			}
			start += (chunksize * qnum);

			qfutures = fjp.invokeAll(qcollect);

			w = 0;
			while (fjp.getActiveThreadCount() > 0 && fjp.isQuiescent() == false) {
				w++;
			}
			log.info("Waited for " + w + " cycles");

			for (Future<ArrayList<String>> f : qfutures) {
				try {

					ArrayList<String> test = f.get();
					if (test != null) {
						if (test.size() > 0) {
							pages.addAll(test);
						}
					}

					if (f.isDone() == false) {
						f.cancel(true);
					}

					f = null;
				} catch (Exception e) {
					log.warn("Encoding Error!");
					e.printStackTrace();
				}
			}
			qcollect = new HashSet<Callable<ArrayList<String>>>(4);
			qfutures = null;
			log.info("Finished Getting Pages");

			// if no records then get records that may have been dropped
			if (pages.size() == 0 && checkstring != null && checkstring.trim().length() > 0 && checkattempts < reattempts) {
				checkattempts += 1;
				log.info("Checking for Drops");
				qcollect.add(new SplitQuery(template, (checkstring)));
				qfutures = fjp.invokeAll(qcollect);

				w = 0;
				while (fjp.getActiveThreadCount() > 0
						&& fjp.isQuiescent() == false) {
					w++;
				}
				log.info("Waited for " + w + " cycles");

				for (Future<ArrayList<String>> f : qfutures) {
					try {

						ArrayList<String> test = f.get();
						if (test != null) {
							if (test.size() > 0) {
								pages.addAll(test);
							}
						}

						if (f.isDone() == false) {
							f.cancel(true);
						}

						f = null;
					} catch (Exception e) {
						log.warn("Encoding Error!");
						e.printStackTrace();
					}
				}
				qfutures = null;
				qcollect = new HashSet<Callable<ArrayList<String>>>(4);

			} else if (checkattempts >= reattempts) {
				pages.clear();
			}

			log.info("Found " + pages.size() + " records!");

			// get hashes if necessary
			if (getHash) {
				log.info("Hashing " + pages.size() + " Records");

				ArrayList<String> hashedrows = new ArrayList<String>();
				for (String row : pages) {

					collect.add(new CreateHash(row, pid));
					pid++;

				}

				log.info("Invoking");
				futures = fjp.invokeAll(collect);

				w = 0;
				while (fjp.getActiveThreadCount() > 0
						&& fjp.isQuiescent() == false) {
					w++;
				}

				log.info("Waited " + w + " Cycles!");

				for (Future<String> f : futures) {
					if (f != null) {
						String json;
						try {
							json = f.get(termtime, TimeUnit.MILLISECONDS);

							if (json != null) {
								hashedrows.add(json);
							}

						} catch (Exception e) {
							log.warn("Encoding Error!");
							e.printStackTrace();
						}
					}

				}
				log.info("Hashed " + hashedrows.size() + " Records!");
				pages = hashedrows;

				collect = new HashSet<Callable<String>>(pullsize);
				futures.clear();
				log.info("Completed Hashing");
			}

			log.info("Performing Regex");
			// handle single patterns
			int i = 0;
			if (singlepats != null) {

				log.info("Found Singlepats");
				int subs = 0;
				int rows = 0;
				for (String row : pages) {
					rows += 1;
					String inrow = row;
					try {

						inrow = inrow.replaceAll("\t|\r|\r\n|\n", "");

						Map<String, Json> jmap = Json.read(inrow).asJsonMap();

						if (singlepats.containsKey("table")) {
							subs += 1;

							if (fjp.isShutdown()) {
								fjp = new ForkJoinPool((Runtime.getRuntime()
										.availableProcessors() * procnum));
							}

							if (jmap.get(column) != null) {

								if (test) {
									System.out
											.println("//////////////////////HTML////////////////////////\n"
													+ jmap.get(column)
															.asString()
													+ "\n///////////////////////////////END///////////////////////////\n\n");
								}

								if (mustcontain != null) {
									if (jmap.get(column).asString()
											.contains(mustcontain)) {
										if (cannotcontain != null) {
											if (jmap.get(column).asString()
													.contains(cannotcontain) == false)
												collect.add(new ParsePage(
														unescape,
														replacementPattern,
														singlepats.get("table"),
														jmap.get(column)
																.asString()
																.replaceAll(
																		"\\s\\s",
																		" "),
														singlepats,
														Calendar.getInstance()
																.getTime()
																.toString(),
														jmap.get("offenderhash")
																.asString()));
										} else {
											collect.add(new ParsePage(unescape,
													replacementPattern,
													singlepats.get("table"),
													jmap.get(column)
															.asString()
															.replaceAll(
																	"\\s\\s",
																	" "),
													singlepats, Calendar
															.getInstance()
															.getTime()
															.toString(),
													jmap.get("offenderhash")
															.asString()));
										}
									}
								} else if (cannotcontain != null) {
									if (jmap.get(column).asString()
											.contains(cannotcontain) == false) {
										collect.add(new ParsePage(unescape,
												replacementPattern, singlepats
														.get("table"), jmap
														.get(column)
														.asString()
														.replaceAll("\\s\\s",
																" "),
												singlepats, Calendar
														.getInstance()
														.getTime().toString(),
												jmap.get("offenderhash")
														.asString()));
									}
								} else {
									collect.add(new ParsePage(unescape,
											replacementPattern, singlepats
													.get("table"), jmap
													.get(column).asString()
													.replaceAll("\\s\\s", " "),
											singlepats, Calendar.getInstance()
													.getTime().toString(), jmap
													.get("offenderhash")
													.asString()));
								}
							}
						}
						i++;

						if (((i % commit_size) == 0 & i != 0)
								|| i == pages.size() || pages.size() == 1
								&& singlepats != null) {
							log.info("Getting Regex Results");

							log.info("Getting Tasks");

							futures = fjp.invokeAll(collect);

							w = 0;

							while (fjp.getActiveThreadCount() > 0
									&& fjp.isQuiescent() == false) {
								w++;
							}

							log.info("Waited for " + w + " cycles");

							for (Future<String> r : futures) {
								try {

									add = r.get();
									if (add.contains("No Data") == false) {
										parsedrows.add(add);
									}

									add = null;

								} catch (Exception e) {
									log.warn("Encoding Error!");
									e.printStackTrace();
								}
							}

							futures = null;
							collect = new HashSet<Callable<String>>();

							if (parsedrows.size() >= commit_size) {
								log.info("INSERTING " + parsedrows.size()
										+ " records!");
								if (parsedrows.size() >= SPLITSIZE) {
									sendToDb(parsedrows, true);
								} else {
									sendToDb(parsedrows, false);
								}

								parsedrows = new ArrayList<String>(pullsize);
							}

							// hint to the gc in case it actually pays off; use
							// -X:compactexplicitgc to improve odds and
							// -XX:UseConcMarkSweepGC for improving odds on
							// older generation strings
							// (think if i were a gambling man)
							System.gc();
							Runtime.getRuntime().gc();
						}
					} catch (Exception e) {
						log.warn("Encoding Error!");
						e.printStackTrace();
					}
				}
				log.info("Submitted " + subs + " records. Found " + rows
						+ " rows");
			}

			log.info("REMAINING ROWS TO COMMIT " + parsedrows.size());
			log.info("Rows Left" + parsedrows.size());
			if (parsedrows.size() > 0) {

				if (parsedrows.size() >= SPLITSIZE) {
					sendToDb(parsedrows, true);
				} else {
					sendToDb(parsedrows, false);
				}

				parsedrows = new ArrayList<String>();
			}

			// handle multi patterns
			if (multipats != null) {
				// parse multiple pages for the run
				int subs = 0;
				for (String row : pages) {
					try {
						for (String k : multipats.keySet()) {
							if (fjp.isShutdown()) {

								fjp = new ForkJoinPool(Runtime.getRuntime()
										.availableProcessors());
							}

							Map<String, Json> jmap = Json.read(row).asJsonMap();

							if (jmap.get(column) != null) {
								subs += 1;
								if (test) {
									System.out
											.println("//////////////////////HTML////////////////////////\n"
													+ jmap.get(column)
															.asString()
													+ "\n///////////////////////////////END///////////////////////////\n\n");
								}

								if (mustcontain != null) {
									if (jmap.get(column).asString()
											.contains(mustcontain)) {
										if (cannotcontain != null) {
											if (jmap.get(column).asString()
													.contains(cannotcontain) == false) {
												collect.add(new ParseMultiPage(
														unescape,
														replacementPattern,
														k,
														jmap.get(column)
																.asString()
																.replaceAll(
																		"\\s\\s",
																		" "),
														jmap.get("offenderhash")
																.asString(),
														Calendar.getInstance()
																.getTime()
																.toString(),
														multipats.get(k)));
											}
										} else {
											collect.add(new ParseMultiPage(
													unescape,
													replacementPattern, k, jmap
															.get(column)
															.asString(),
													jmap.get("offenderhash")
															.asString()
															.replaceAll(
																	"\\s\\s",
																	" "),
													Calendar.getInstance()
															.getTime()
															.toString(),
													multipats.get(k)));
										}
									}
								} else if (cannotcontain != null) {
									if (jmap.get(column).asString()
											.contains(cannotcontain) == false) {
										collect.add(new ParseMultiPage(
												unescape, replacementPattern,
												k, jmap.get(column)
														.asString()
														.replaceAll("\\s\\s",
																" "), jmap.get(
														"offenderhash")
														.asString(), Calendar
														.getInstance()
														.getTime().toString(),
												multipats.get(k)));
									}

								} else {
									collect.add(new ParseMultiPage(
											unescape,
											replacementPattern,
											k,
											jmap.get(column).asString()
													.replaceAll("\\s\\s", " "),
											jmap.get("offenderhash").asString(),
											Calendar.getInstance().getTime()
													.toString(), multipats
													.get(k)));
								}
							}

							i++;
							if (((i % commit_size) == 0 & i != 0)
									|| i == pages.size() || pages.size() == 1
									&& multipats != null) {
								futures = fjp.invokeAll(collect);
								w = 0;
								while (fjp.getActiveThreadCount() > 0
										&& fjp.isQuiescent() == false) {
									w++;
								}

								log.info("Waited " + w + " Cycles");

								for (Future<String> r : futures) {
									try {
										add = r.get();

										if (add.contains("No Data") == false) {

											for (String js : add.split("~")) {
												parsedrows.add(js);
											}
										}
										add = null;

										if (r.isDone() == false) {
											r.cancel(true);
										}
										r = null;

									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (ExecutionException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}

								futures = null;
								collect = new HashSet<Callable<String>>();

								if (parsedrows.size() >= commit_size) {
									log.info("INSERTING " + parsedrows.size()
											+ " records!");
									if (parsedrows.size() >= SPLITSIZE) {
										sendToDb(parsedrows, true);
									} else {
										sendToDb(parsedrows, false);
									}
									parsedrows = new ArrayList<String>(pullsize);
								}

								// hint to the gc in case it actually pays off
								System.gc();
								Runtime.getRuntime().gc();
							}
						}

					} catch (Exception e) {
						log.warn("Encoding Error!");
					}

				}
				log.info("Submitted " + subs + " records.");
			}

			// handle looped patterns
			if (loopedpats != null) {
				log.info("Looped Patterns Found");
				int subs = 0;
				if (fjp.isShutdown()) {
					fjp = new ForkJoinPool(Runtime.getRuntime()
							.availableProcessors() * procnum);
				}

				for (String row : pages) {
					try {

						for (String k : loopedpats.keySet()) {
							if (fjp.isShutdown()) {
								fjp = new ForkJoinPool(Runtime.getRuntime()
										.availableProcessors() * procnum);
							}
							Map<String, Json> jmap = Json.read(row).asJsonMap();

							if (jmap.get(column) != null) {
								subs += 1;
								if (mustcontain != null) {
									if (jmap.get(column).asString()
											.contains(mustcontain)) {
										if (cannotcontain != null) {
											if (jmap.get(column).asString()
													.contains(cannotcontain) == false) {
												collect.add(new LoopRegex(
														unescape,
														jmap.get(column)
																.asString()
																.replaceAll(
																		"\\s\\s",
																		" "),
														jmap.get("offenderhash")
																.asString(),
														Calendar.getInstance()
																.getTime()
																.toString(), k,
														replacementPattern,
														loopedpats.get(k), test));
											}
										} else {
											collect.add(new LoopRegex(unescape,
													jmap.get(column)
															.asString()
															.replaceAll(
																	"\\s\\s",
																	" "),
													jmap.get("offenderhash")
															.asString(),
													Calendar.getInstance()
															.getTime()
															.toString(), k,
													replacementPattern,
													loopedpats.get(k), test));
										}
									}
								} else if (cannotcontain != null) {
									if (jmap.get(column).asString()
											.contains(cannotcontain) == false) {
										collect.add(new LoopRegex(unescape,
												jmap.get(column)
														.asString()
														.replaceAll("\\s\\s",
																" "), jmap.get(
														"offenderhash")
														.asString(), Calendar
														.getInstance()
														.getTime().toString(),
												k, replacementPattern,
												loopedpats.get(k), test));
									}
								} else {
									collect.add(new LoopRegex(unescape, jmap
											.get(column).asString()
											.replaceAll("\\s\\s", " "), jmap
											.get("offenderhash").asString(),
											Calendar.getInstance().getTime()
													.toString(), k,
											replacementPattern, loopedpats
													.get(k), test));
								}
								jmap.remove(k);
							}
							i++;
							if (((i % commit_size) == 0 & i != 0)
									|| (i % (pages.size() - 1)) == 0
									|| pages.size() == 1) {

								futures = fjp.invokeAll(collect);

								w = 0;

								while (fjp.getActiveThreadCount() > 0
										&& fjp.isQuiescent() == false) {
									w++;
								}
								log.info("Waited " + w + " Cycles");

								for (Future<String> r : futures) {
									try {
										add = r.get();
										if (add.contains("No Data") == false) {
											for (String toarr : add.split("~")) {
												parsedrows.add(toarr);
											}
										}

										if (r.isDone() == false) {
											r.cancel(true);
										}
										add = null;

									} catch (Exception e) {
										log.warn("Encoding Error!");
										e.printStackTrace();
									}
								}

								futures = null;
								collect = new HashSet<Callable<String>>();

								// hint to the gc in case it actually pays off
								System.gc();
								Runtime.getRuntime().gc();
							}
						}

						if (parsedrows.size() >= this.commit_size) {
							log.info("INSERTING " + parsedrows.size()
									+ " records!");
							if (parsedrows.size() >= SPLITSIZE) {
								sendToDb(parsedrows, true);
							} else {
								sendToDb(parsedrows, false);
							}

							parsedrows = new ArrayList<String>(pullsize);
						}

					} catch (Exception e) {
						log.warn("Encoding Error!");
					}
				}
				log.info("Submitted " + subs + " records.");
			}

			if (collect.size() > 0) {
				log.info("Getting Last Regex Results for Iteration");

				log.info("Getting Tasks");

				futures = fjp.invokeAll(collect);

				w = 0;

				while (fjp.getActiveThreadCount() > 0
						&& fjp.isQuiescent() == false) {
					w++;
				}

				log.info("Waited for " + w + " cycles");

				for (Future<String> r : futures) {
					try {

						add = r.get();
						if (add.contains("No Data") == false) {
							parsedrows.add(add);
						}

						add = null;

					} catch (Exception e) {
						log.warn("Encoding Error!");
						e.printStackTrace();
					}
				}

				futures = null;
				collect = new HashSet<Callable<String>>(pullsize);
				// hint to the gc in case it actually pays off; use
				// -X:compactexplicitgc to improve odds and
				// -XX:UseConcMarkSweepGC for improving odds on older generation
				// strings
				// (think if i were a gambling man)
				System.gc();
				Runtime.getRuntime().gc();
			}

			log.info("REMAINING ROWS TO COMMIT " + parsedrows.size());
			log.info("Rows Left" + parsedrows.size());
			if (parsedrows.size() > 0) {

				if (parsedrows.size() >= SPLITSIZE) {
					sendToDb(parsedrows, true);
				} else {
					sendToDb(parsedrows, false);
				}

				parsedrows = new ArrayList<String>();
			}

		} while (pages != null && pages.size() > 0);

		// ensure that nothing is still caught in limbo
		// final parser to ensure that nothing is left out
		if (collect.size() > 0) {
			log.info("More Rows Caught in FJP, Completing Process");
			futures = fjp.invokeAll(collect);

			w = 0;

			while (fjp.getActiveThreadCount() > 0 && fjp.isQuiescent() == false) {
				w++;
			}
			log.info("Waited " + w + " Cycles");

			for (Future<String> r : futures) {
				try {
					add = r.get();

					if (add.contains("No Data") == false) {

						for (String js : add.split("~")) {
							parsedrows.add(js);
						}
					}
					add = null;

					if (r.isDone() == false) {
						r.cancel(true);
					}
					r = null;

				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			futures = null;
			collect = null;
		}

		// send any remaining parsed rows to the db
		if (parsedrows.size() > 0) {

			if (parsedrows.size() >= SPLITSIZE) {
				sendToDb(parsedrows, true);
			} else {
				sendToDb(parsedrows, false);
			}

			parsedrows = new ArrayList<String>();
		}

		log.info("Shutting Down Fork Join Pool");
		if (fjp.isShutdown() == false) {
			fjp.shutdownNow();
		}

		fjp = null;

		log.info("Complete @" + Calendar.getInstance().getTime().toString());
		log.info("Total Runtime(seconds): "
				+ Double.toString((double) (Calendar.getInstance()
						.getTimeInMillis() - t) / 1000));

		// hint to the gc in case it actually pays off
		System.gc();
		Runtime.getRuntime().gc();
	}
}
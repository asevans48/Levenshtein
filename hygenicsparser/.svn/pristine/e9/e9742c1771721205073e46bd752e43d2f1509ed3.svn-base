package com.hygenics.parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;

import mjson.Json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hygenics.exceptions.MissingPropertyException;

public class DumptoText {

	private final Logger log = LoggerFactory.getLogger(MainApp.class);

	private String notnull;
	private List<String> columns;
	private String hashvar;
	private Collection<String> hashstring = new HashSet<String>();
	private int commit_size = 100;
	private String offsetid;
	private int offset = 0;
	private ArrayList<String> rows;
	private String fpath;
	private getDAOTemplate template;
	private String select;

	public DumptoText() {

	}

	protected static class Queue {
		private ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();

		private static class Instance {
			private static final Queue Instance = new Queue();
		}

		public static Queue getInstance() {
			return Instance.Instance;
		}

		public void close() {
			Set<String> k = map.keySet();

			for (String key : k) {
				map.remove(key);
			}

			k = null;
			map = null;
		}

		public void addtoQueue(String fpath, String buf) {
			map.put(fpath, buf);
		}

		public void resetQueue() {
			map = new ConcurrentHashMap<String, String>();
		}

		public int getSize() {
			return map.size();
		}

		public String getFromQueue() {
			Set<String> keys = map.keySet();
			Iterator<String> it = keys.iterator();
			String ret = null;
			if (it.hasNext()) {
				ret = it.next();

				if (ret.compareTo("CLOSE") == 0) {
					if (keys.size() > 1) {
						ret = it.next();
						String b = map.get(ret);
						map.remove(ret);
						return b;
					} else {
						map.remove(ret);
						return map.get(ret);
					}
				}

				String b = map.get(ret);
				map.remove(ret);
				return b;
			}
			return null;
		}
	}

	public class Write implements Runnable {
		private final String fpath;
		private FileWriter fw;
		private ByteBuffer buf;
		private final Queue q;

		public Write(final String fpath, Queue q) {
			// file writer came out faster than nio for this task
			this.q = q;
			this.fpath = fpath;
			try {
				fw = new FileWriter(new File(fpath));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void run() {
			boolean foundclose = false;
			boolean run = true;
			String msg;
			do {
				msg = q.getFromQueue();

				if (msg != null) {
					try {

						if (msg.compareTo("CLOSE") == 0) {
							foundclose = true;
						} else {
							fw.write((msg));
						}

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (foundclose == true & q.getSize() == 0) {
					run = false;
				}
				msg = null;

			} while (run);

			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * The Tokenize class for file writing. All columns should be specified in
	 * the order they are to be printed to ensure standardization and eradicate
	 * any scrambling which can occur.
	 */
	public class Tokenize extends RecursiveTask<String> {

		private final String str;
		private final List<String> columns;

		public Tokenize(final String str, final List<String> columns) {
			this.str = str;
			this.columns = columns;
		}

		@Override
		public String compute() {
			// TODO Auto-generated method stub
			String tokenString = null;
			Map<String, Json> jmap = Json.read(this.str).asJsonMap();

			for (int i = 0; i < columns.size(); i++) {

				tokenString = (tokenString == null) ? jmap.get(columns.get(i))
						.asString() : tokenString + "|"
						+ jmap.get(columns.get(i)).asString();

			}

			jmap = null;

			return tokenString;
		}
	}

	public String getNotnull() {
		return notnull;
	}

	public void setNotnull(String notnull) {
		this.notnull = notnull;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public String getOffsetid() {
		return offsetid;
	}

	public void setOffsetid(String offsetid) {
		this.offsetid = offsetid;
	}

	public String getHashvar() {
		return hashvar;
	}

	public void setHashvar(String hashvar) {
		this.hashvar = hashvar;
	}

	public ArrayList<String> getRows() {
		return rows;
	}

	public void setRows(ArrayList<String> rows) {
		this.rows = rows;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public String getFpath() {
		return fpath;
	}

	public void setFpath(String fpath) {
		this.fpath = fpath;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public String getSelect() {
		return select;
	}

	public void setSelect(String select) {
		this.select = select;
	}

	public int getCommit_size() {
		return commit_size;
	}

	public void setCommit_size(int commit_size) {
		this.commit_size = commit_size;
	}

	/**
	 * Get the Data
	 * 
	 * @param condition
	 */
	public void getData(String condition) {
		rows = this.template.getJsonData((select.trim() + condition));
	}

	/**
	 * Write Remaining Records
	 * 
	 * @param q
	 */
	private void writeRemaining(Queue q) {
		FileWriter fw = null;
		String line = null;

		try {
			fw = new FileWriter(new File(fpath), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		do {
			if (fw != null)
				line = q.getFromQueue();

			if (line != null) {
				try {
					fw.write((line));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} while (q.getSize() > 0 & fw != null & line != null);

		if (fw != null) {
			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void run() {
		// original fjp setup
		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors() * 2);
		Collection<ForkJoinTask<String>> tasks = new HashSet<ForkJoinTask<String>>();
		ArrayList<String> lines = new ArrayList<String>();
		Thread t = null;
		int recs = 0;

		// generate q
		Queue q = Queue.getInstance();
		q.resetQueue();

		if (columns != null) {

			if (columns.size() == 0) {
				try {
					throw new MissingPropertyException(
							"DumptoText ERROR: Missing Column Names");
				} catch (MissingPropertyException e) {
					e.printStackTrace();
				}
			}

			String cols = null;

			for (String c : columns) {
				cols = (cols == null) ? c : cols + "|" + c;
			}

			// add date to fpath
			fpath += Calendar.getInstance().getTime().toString()
					.replaceAll("\\:", "_");
			fpath = fpath.replaceAll("\\s", "_");
			fpath += ".txt";

			// NIO doesn't create a file
			File f = new File(fpath);

			if (f.exists()) {
				f.delete();
			}

			try {
				f.createNewFile();
				f.setReadable(true);
				f.setWritable(true);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			cols += "\n";

			q.addtoQueue(fpath, cols);

			// writer for the file
			Write w = new Write(fpath, q);
			t = new Thread(w);
			t.start();

			// for hashing
			Map<String, Json> jmap;

			String tokenstr = null;

			// pullid setup
			String condition = " WHERE " + offsetid + " >= " + offset + " AND "
					+ offsetid + " < " + (offset + commit_size);
			getData(condition);

			int j = 0;

			while (rows.size() > 0) {
				if (fjp.isShutdown()) {
					fjp = new ForkJoinPool((Runtime.getRuntime()
							.availableProcessors() * 2));
				}

				for (String r : rows) {
					jmap = Json.read(r).asJsonMap();
					if (hashvar != null) {

						if (hashstring.contains(jmap.get(hashvar).asString()
								.trim()) == false) {

							if (notnull != null) {
								if (jmap.get(notnull).asString().length() > 0) {
									tasks.add(fjp.submit(new Tokenize(r
											.replaceAll("&amp;|&amp", ""),
											columns)));
									hashstring.add(jmap.get(hashvar).asString()
											.trim());
								}
							} else {
								tasks.add(fjp.submit(new Tokenize(r.replaceAll(
										"&amp;|&amp", ""), columns)));
								hashstring.add(jmap.get(hashvar).asString()
										.trim());
							}
						}
					} else {
						if (notnull != null) {
							if (jmap.get(notnull).asString().length() > 0) {
								tasks.add(fjp.submit(new Tokenize(r.replaceAll(
										"&amp;|&amp", ""), columns)));
							}
						} else {
							tasks.add(fjp.submit(new Tokenize(r.replaceAll(
									"&amp;|&amp", ""), columns)));
						}
					}
				}

				try {
					fjp.awaitTermination(500, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				log.info("Shutdown");
				fjp.shutdown();

				int incrementor = 0;
				while (fjp.isTerminated()) {
					incrementor++;
				}

				for (ForkJoinTask<String> task : tasks) {
					try {

						if (task != null) {
							tokenstr = task.get();
						}

						if (tokenstr != null) {
							tokenstr += "\n";
							q.addtoQueue((fpath + "|" + recs), tokenstr);
							recs++;
						}

						tokenstr = null;
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				tasks = new HashSet<ForkJoinTask<String>>();

				// get more rows
				rows = new ArrayList<String>();
				j++;

				condition = " WHERE " + offsetid + " >= "
						+ Integer.toString(offset + (commit_size * j))
						+ " AND " + offsetid + " < "
						+ Integer.toString(offset + (commit_size * (j + 1)));
				getData(condition);
			}
		} else {
			try {
				throw new MissingPropertyException(
						"DumptoText ERROR: No Columns Specified");
			} catch (MissingPropertyException e) {
				e.printStackTrace();
			}
		}

		log.info("Records Output: " + recs);

		log.info("Ending Thread");

		q.addtoQueue(fpath, "CLOSE");

		if (fjp.isShutdown() == false) {
			fjp.shutdown();
		}

		log.info("Await Termination");
		recs = 0;
		while (t.isAlive() == true) {
			recs++;
		}

		log.info("Waited " + recs + " Cycles");

		if (q.getSize() > 0) {
			log.info("Writing Remaining " + q.getSize() + " Records");
			writeRemaining(q);
		}

		q.close();
		q = null;
		hashstring = null;
		log.info("Write Complete");
	}

}

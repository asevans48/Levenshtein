package com.hygenics.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.activation.DataHandler;
import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

import mjson.Json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;

import com.eclipsesource.json.JsonObject;

public class MailDrops {
	private Logger log = LoggerFactory.getLogger(MainApp.class.getName());
	private getDAOTemplate template;
	private List<String> addresses;
	private String logpath;
	private String source;
	private String regex;
	private int procnum;
	private String table;
	private int maxsize = 200;
	private String mailhost;
	private String username;
	private String password;

	private int commitsize = 100;

	public MailDrops() {
		procnum = 2;
	}

	public int getMaxsize() {
		return maxsize;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getMailhost() {
		return mailhost;
	}

	public void setMailhost(String mailhost) {
		this.mailhost = mailhost;
	}

	public void setMaxsize(int maxsize) {
		this.maxsize = maxsize;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public int getCommitsize() {
		return commitsize;
	}

	public void setCommitsize(int commitsize) {
		this.commitsize = commitsize;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public int getProcnum() {
		return procnum;
	}

	public void setProcnum(int procnum) {
		this.procnum = procnum;
	}

	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;
	}

	public List<String> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<String> addresses) {
		addresses = addresses;
	}

	public String getLogpath() {
		return logpath;
	}

	public void setLogpath(String logpath) {
		this.logpath = logpath;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	private class InsertUrls extends RecursiveAction {
		private final List<String> urls;
		private final String table;
		private final String source;

		InsertUrls(String source, List<String> urls, String table) {
			this.urls = urls;
			this.table = table;
			this.source = source;
		}

		@Override
		protected void compute() {
			// TODO Insert the urls that could not be obtained into the database
			ArrayList<String> jsonarr = new ArrayList<String>();
			for (String url : urls) {
				jsonarr.add(new JsonObject().add("source", source)
						.add("url", url).toString());
			}

			template.postJsonData("INSERT INTO " + table
					+ " (source,url) VALUES(?,?)", jsonarr);
		}

	}

	/**
	 * Class that Parses a Specific Log from a specified path and file
	 * 
	 * @author aevans
	 *
	 */
	private class ParseLog implements Callable<ArrayList<String>> {

		private final String file;
		private final String regex;

		ParseLog(String file, String regex) {
			this.file = file;
			this.regex = regex;
		}

		@Override
		public ArrayList<String> call() {
			// TODO parse the log file and return an
			// HashMap<String,ArrayList<String>> of urls
			ArrayList<String> urls = new ArrayList<String>(100);

			try (BufferedReader reader = new BufferedReader(new FileReader(
					new File(this.file)))) {
				String line;
				Pattern p = Pattern.compile(this.regex);

				while ((line = reader.readLine()) != null) {
					Matcher m = p.matcher(line);
					urls.add(m.group().replace("&amp;", "&"));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return urls;
		}

	}

	/**
	 * Get an ArrayList of all files
	 * 
	 * @param path
	 * @return
	 */
	private ArrayList<String> getFiles(String path) {
		String[] files = new File(path).list();
		ArrayList<String> retfiles = new ArrayList<String>(files.length);
		for (String file : files) {
			if (new File(file).isDirectory()) {
				retfiles.addAll(getFiles(file));
			}
		}
		return retfiles;
	}

	/**
	 * Checks if the dropped records size is greater than the email size and
	 * emails the dropped records. A separate script will email automatically
	 * after a month via the Python auto controller.
	 * 
	 * @throws UnsupportedEncodingException
	 */
	private void checkEmail() throws UnsupportedEncodingException {
		if (template.queryForInt("SELECT count(distinct(url)) FROM " + table) > maxsize) {
			String csv = "source,url\r\n";
			log.info("Building CSV");
			ArrayList<String> jsons = template
					.getJsonData("SELECT source,url FROM " + table);
			StringWriter sw = new StringWriter();
			CSVWriter writer = new CSVWriter(sw, '\n');
			for (int i = 0; i < jsons.size(); i++) {
				Map<String, Json> obj = Json.read(jsons.get(i)).asJsonMap();
				writer.writeNext((obj.get("source") + "#" + obj.get("url"))
						.split("#"));
			}

			csv = sw.toString();

			log.info("Building Message");

			Properties props = new Properties();
			props.put("mail.smtp.auth", "true");
			props.put("mail.smtp.starttls.enable", "true");
			props.put("mail.smtp.host", mailhost);
			props.put("mail.smtp.port", "25");

			Session session;
			if (username != null && password != null) {
				session = Session.getInstance(props,
						new javax.mail.Authenticator() {
							@Override
							protected PasswordAuthentication getPasswordAuthentication() {
								return new PasswordAuthentication(username,
										password);
							}
						});
			} else {
				session = Session.getInstance(props);
			}

			try {
				Message message = new MimeMessage(session);
				Address[] addr = new Address[addresses.size()];
				for (int i = 0; i < addresses.size(); i++) {
					addr[i] = new InternetAddress(addresses.get(i));
				}

				message.addRecipients(Message.RecipientType.TO, addr);
				message.setFrom(new InternetAddress(
						"Jake.Franklin@hygenicsdata.com"));
				message.setSubject(source
						+ " Has Failed to Acquire Certain Records.See att.");
				message.setText(source
						+ " has failed to acquire over "
						+ maxsize
						+ " records. Attached is an excel sheet of the records. The url list will be recreated for each run.");

				MimeBodyPart messageBodyPart = new MimeBodyPart();
				Multipart multipart = new MimeMultipart();

				messageBodyPart.setDataHandler(new DataHandler(
						new ByteArrayDataSource(csv.getBytes("utf-8"),
								"application/octet-stream")));
				messageBodyPart.setFileName("failedURLS.csv");
				multipart.addBodyPart(messageBodyPart);
				message.setContent(multipart);

				log.info("Sending Message to Recipients");
				message.setContent(multipart);
				Transport.send(message);
				log.info("Message Sent Succesfully.");
			} catch (MessagingException e) {
				log.info("Could Not Send Email!");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Run the Log Parser and Mail Sender
	 */
	public void run() {
		// TODO Run the Log parser and call checkEmail()
		// parse the logs and submit to database
		log.info("Starting Failed URL Handling @ "
				+ Calendar.getInstance().getTime().toString());
		log.info("Clearing Current List");
		template.execute("DELETE FROM " + table + " WHERE source LIKE "
				+ source.trim());
		log.info("Parsing Urls from All Found Log Files");
		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors() * procnum);
		Set<Callable<ArrayList<String>>> futures = new HashSet<Callable<ArrayList<String>>>(
				100);
		List<Future<ArrayList<String>>> returls;
		ArrayList<String> urls = new ArrayList<String>();
		ArrayList<String> files = getFiles(logpath);

		for (String file : files) {
			futures.add(new ParseLog(file, this.regex));

			if (futures.size()
					% (procnum * Runtime.getRuntime().availableProcessors()) == 0) {
				returls = fjp.invokeAll(futures);
				urls = new ArrayList<String>(returls.size());

				int w = 0;
				while (fjp.isQuiescent() == false
						&& fjp.getActiveThreadCount() > 0) {
					w++;
				}
				log.info("Waited for " + w + " cycles");

				for (Future<ArrayList<String>> f : returls) {
					try {
						urls.addAll(f.get());
					} catch (InterruptedException e) {
						// TODO Catch the Interrupted exception from the thread
						e.printStackTrace();
					} catch (ExecutionException e) {
						// TODO failed fjp execution
						e.printStackTrace();
					}
				}

				if (urls.size() >= commitsize) {
					int start = 0;
					int end = (int) Math.floor(commitsize / 3);

					for (int i = 0; i < 4; i++) {
						if (i == 3) {
							fjp.execute(new InsertUrls(source, urls.subList(
									start, end), table));
						}
					}

					w = 0;

					while (fjp.isQuiescent() == false
							&& fjp.getActiveThreadCount() > 0) {
						w++;
					}
					log.info("Waited for " + w + " cycles");
				}
			}
		}

		if (urls != null && urls.size() > 0) {
			int start = 0;
			int end = (int) Math.floor(commitsize / 3);

			for (int i = 0; i < 4; i++) {
				if (i == 3) {
					fjp.execute(new InsertUrls(source,
							urls.subList(start, end), table));
				}
			}

			int w = 0;

			while (fjp.isQuiescent() == false && fjp.getActiveThreadCount() > 0) {
				w++;
			}
			log.info("Waited for " + w + " cycles");
		}

		// check on whether or not to email and do so
		log.info("Checking If Email Must Be Sent");
		try {
			checkEmail();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("Failed URL Handling Complete @ "
				+ Calendar.getInstance().getTime().toString());
	}

}
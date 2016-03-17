package com.hygenics.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import com.hygenics.sftp.SFTP;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * This program is for transferring files vai SFTP using the com.hygenics.sftp
 * package as well as the SCP option.
 * 
 * Current Functions include SCP and SFTP.
 * 
 * Some of the SCP code is derived from
 * http://www.jcraft.com/jsch/examples/ScptTo.java.html due to a minimal
 * knowledge of JSch.
 * 
 * @author asevans
 *
 */
public class Upload {

	private Logger log = LoggerFactory.getLogger(MainApp.class);

	public enum UploadTypes {
		SFTP, SCP
	}

	private final int MAXFILESPERTHREAD = 100; // sets a maximum number of files
												// per thread to avoid
												// overworking the threads
	private getDAOTemplate template;
	private String dbCheck;
	private Long timeout = 100000L;
	private int numthreads = 2;
	private UploadTypes TYPE = UploadTypes.SFTP;
	private String server;
	private String user;
	private String pass;
	private String remotePath;
	private String localPath;
	private String keyFile;
	private String host;

	public Upload() {

	}// empty constructor

	public getDAOTemplate getTemplate() {
		return template;
	}// getTemplate

	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}// setTemplate

	public String getDbCheck() {
		return dbCheck;
	}// getDbCheck

	public void setDbCheck(String dbCheck) {
		this.dbCheck = dbCheck;
	}// setDbCheck

	public Long getTimeout() {
		return timeout;
	}// getTimeout

	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}// setTimeout

	public int getNumthreads() {
		return numthreads;
	}// getNumthreads

	public void setNumthreads(int numthreads) {
		this.numthreads = numthreads;
	}// setNumthreads

	public String getUser() {
		return user;
	}// getUser

	public void setUser(String user) {
		this.user = user;
	}// setUser

	public String getServer() {
		return server;
	}// getServer

	public void setServer(String server) {
		this.server = server;
	}// setServer

	public String getPass() {
		return pass;
	}// getPass

	public void setPass(String pass) {
		this.pass = pass;
	}// setPass

	public String getRemotePath() {
		return remotePath;
	}// getRemotePath

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}// setRemotePath

	public String getLocalPath() {
		return localPath;
	}// getLocalPath

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}// setLocalPath

	public String getKeyFile() {
		return keyFile;
	}// getKeyFile

	public void setKeyFile(String keyFile) {
		this.keyFile = keyFile;
	}// setKeyFile

	public UploadTypes getTYPE() {
		return TYPE;
	}// getTYPE

	public void setTYPE(UploadTypes TYPE) {
		this.TYPE = TYPE;
	}// setTYPE

	public String getHost() {
		return host;
	}// getHost

	public void setHost(String host) {
		this.host = host;
	}// sethost

	private Map<String, ArrayList<String>> addFiles(File fp, String path,
			SFTP sftp) {
		Map<String, ArrayList<String>> mapping = new HashMap<String, ArrayList<String>>();
		ArrayList<String> files = new ArrayList<String>();
		String opath = path;

		for (String f : fp.list()) {
			if (new File(f).isDirectory()) {
				sftp.make(path + "/" + f);
				mapping.putAll(addFiles(new File(f), opath + "/" + f, sftp));
			} else {
				files.add(f);
			}
		}
		mapping.put(path, files);

		return mapping;
	}

	private void doSftp() {
		SFTP sftp = new SFTP(remotePath, localPath, user, pass, host);
		File fp = new File(localPath);
		Map<String, ArrayList<String>> files = null;

		if (fp.isDirectory()) {
			files = addFiles(fp, remotePath, sftp);
		} else {
			files = new HashMap<String, ArrayList<String>>(1);
			ArrayList<String> arr = new ArrayList<String>(1);
			arr.add(localPath);
			files.put(remotePath, arr);
		}

		for (String key : files.keySet()) {
			// check for directory status
			Pattern p = Pattern.compile("\\/.*?\\.[A-Za-z]+");
			Matcher m = p.matcher(key);
			boolean isDir = m.find();

			// add files
			for (String f : files.get(key)) {
				if (isDir) {
					sftp.set_path(key + "/" + f.replaceAll(".*?\\/", ""));
				} else {
					sftp.set_path(key);
				}
				sftp.set_fpath(f);
				sftp.upload(f);
			}
		}
	}// doSftp

	private class SCP implements Runnable {
		private final List<String> files;
		private final String remotePath;

		public SCP(List<String> files, String remotePath) {
			this.files = files;
			this.remotePath = remotePath;
		}

		@Override
		public void run() {
			// some of this code comes from
			// www.jcraft.com/jsch/examples/ScptTo.java.html
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");

			log.info("Number of Files: " + files.size());

			try {
				JSch jsch = new JSch();
				Session session;
				session = jsch.getSession(user, host, 22);
				session.setPassword(pass);
				// configure jsch
				session.setConfig("StrictHostKeyChecking", "no");
				// connect
				session.connect();

				for (String f : files) {
					boolean uFile = true;
					if (dbCheck != null) {
						log.info("Checking for proper existance for " + f);
						if (template.getJsonData(dbCheck.replace("$FILE$", f))
								.size() == 0) {
							uFile = false;
						}
					}

					if (uFile) {
						String[] splf = f.split("\\/");
						log.info(f + " to " + this.remotePath
								+ splf[splf.length - 1]);

						if (session.isConnected()) {
							Channel channel = session.openChannel("exec");
							String cmd = "scp -t " + this.remotePath
									+ splf[splf.length - 1];
							((ChannelExec) channel).setCommand(cmd);

							OutputStream out = channel.getOutputStream();
							InputStream in = channel.getInputStream();

							channel.connect();

							if (channel.isConnected()) {

								File fo = new File(f);
								long fsz = fo.length();

								cmd = "C0644 " + fsz + " ";

								if (localPath.lastIndexOf('/') > 0) {
									cmd += localPath.substring(localPath
											.lastIndexOf('/') + 1);
								} else {
									cmd += localPath;
								}

								cmd += "\n";

								out.write(cmd.getBytes());
								out.flush();
								int i = in.read();
								if (i == 0) {

									byte[] b = new byte[1024];
									FileInputStream fis = new FileInputStream(
											fo);
									log.info("File Length: "
											+ Long.toString(fo.length()));
									int len = 0;
									do {
										len = fis.read(b, 0, b.length);
										if (len > 0) {
											out.write(b, 0, len);
											out.flush();
										}
									} while (len >= 0);

									fis.close();
									channel.disconnect();
								} else {
									log.error("JSCH ERROR " + i + " for " + f);
								}
							}
						} else {
							log.error("Could Not Establish Channel!");
						}
					}
				}

				session.disconnect();

			} catch (JSchException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void doScp() {
		ArrayList<String> files = new ArrayList<String>();
		File fp = new File(localPath);

		if (fp.isDirectory()) {
			for (String f : fp.list()) {
				files.add(localPath + f);
			}
		} else {
			files.add(localPath);
		}

		int p = 0;
		int partsize = files.size() / MAXFILESPERTHREAD;
		int offset = 0;

		if (partsize < 100) {
			partsize = files.size();
		}

		ExecutorService exec = Executors.newFixedThreadPool(this.numthreads);

		do {
			List<String> subset = files.subList(offset, offset + partsize);
			exec.execute(new SCP(subset, this.remotePath));

			p++;
			if (p == numthreads) {
				try {
					exec.awaitTermination(timeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				p = 0;
			}
			offset += partsize;
		} while (offset < files.size());

		if (p > 0) {
			try {
				exec.awaitTermination(timeout, TimeUnit.MILLISECONDS);
				exec.shutdown();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}// doScp

	public void run() {
		if (TYPE == UploadTypes.SFTP) {
			doSftp();
		} else if (TYPE == UploadTypes.SCP) {
			doScp();
		}
	}// run -->program entry point

}

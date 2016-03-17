package com.hygenics.sftp;

/*
 * SFTP to a SFTP File Server 
 * 
 * Uses jsch to upload to a file server. Uploads bytes in byte[] or files
 * 
 * aevans 8/5/2013
 */

//for byte upload

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
//for file upload
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

import com.hygenics.exceptions.NullByteArrayException;
import com.jcraft.jsch.ChannelSftp;
//jcraft sftp
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/**
 * SFTP Program that allows for upload and download via sftp server
 * 
 * @author aevans
 *
 */
public class SFTP {

	// byte array
	private byte[] bytes;

	// file path
	private String fpath;

	// directory
	private String path;

	// authentication information
	private String user;
	private String pass;
	private String host;

	// the session information
	private Session session = null;
	private ChannelSftp sftp = null;
	private com.jcraft.jsch.Channel channel = null;

	/**
	 * Empty Constructor
	 */
	public SFTP() {

	}

	/**
	 * Constructor No file path is specified at the time
	 * 
	 * @param inpath
	 * @param inuser
	 * @param inpass
	 * @param inhost
	 */
	public SFTP(String inpath, String inuser, String inpass, String inhost) {
		// TODO a constructor with ftp access

		// directory
		path = inpath;

		// authentication information
		user = inuser;
		pass = inpass;
		host = inhost;
		conn();
	}

	/**
	 * Full Constructor infpath is the file path inpath is the sftp path
	 * 
	 * @param infpath
	 * @param inpath
	 * @param inuser
	 * @param inpass
	 * @param inhost
	 */
	public SFTP(String infpath, String inpath, String inuser, String inpass,
			String inhost) {
		// TODO a constructor with ftp access

		// path
		fpath = infpath;

		// directory
		path = inpath;

		// authentication information
		user = inuser;
		pass = inpass;
		host = inhost;
		conn();
	}

	/**
	 * Full Constructor with file bytes for speed.
	 * 
	 * @param inbytes
	 * @param inpath
	 * @param inuser
	 * @param inpass
	 * @param inhost
	 */
	public SFTP(byte[] inbytes, String inpath, String inuser, String inpass,
			String inhost) {
		// TODO a constructor with ftp access
		bytes = inbytes;
		path = inpath;
		user = inuser;
		pass = inpass;
		host = inhost;
		conn();
	}

	public void set_bytes(byte[] inbytes) {
		bytes = inbytes;
	}

	public void set_fpath(String infpath) {
		// TODO set fpath
		fpath = infpath;
	}

	public String get_fpath() {
		// TODO get fpath
		return fpath;
	}

	public void set_path(String inpath) {
		// TODO set path
		path = inpath;
	}

	public String get_path() {
		// TODO get path
		return path;
	}

	public void set_user(String inuser) {
		// TODO set user
		user = inuser;
	}

	public String get_user() {
		// TODO get user
		return user;
	}

	public void set_pass(String inpass) {
		// TODO set password
		pass = inpass;
	}

	public String get_pass() {
		// TODO get password
		return pass;
	}

	public void set_host(String inhost) {
		// TODO set host
		pass = inhost;
	}

	public String get_host() {
		// TODO get host
		return host;
	}

	/**
	 * SFTP without bytes
	 * 
	 * @param filename
	 */
	public void sftp(String filename) {
		// TODO sftp with file specified (uses filename specified for server)
		sftp_no_bytes(filename);
	}

	/**
	 * SFTP with bytes filename is the filename bytes is the boolean specifying
	 * whether to use bytes
	 * 
	 * @param filename
	 * @param bytes
	 * 
	 */
	public void sftp(String filename, boolean bytes) {
		// TODO sftp with bytes or without as specified(uses specified filename
		// on server)
		if (bytes == true) {
			sftp_bytes(filename);
		} else {
			sftp_no_bytes(filename);
		}
	}

	/**
	 * Make a directory with specified directory
	 * 
	 * @param dir
	 */
	public void mkdir(String dir) {
		// TODO make a non-existing directory
		make(dir);
	}

	/**
	 * Connect using host, port, pass, user
	 */
	public void connect() {
		conn();
	}

	private void conn() {
		JSch jsch = new JSch();

		if (session != null) {
			session.disconnect();
			session = null;
		}

		if (sftp != null) {
			sftp.disconnect();
			sftp = null;
		}

		java.util.Properties config = new java.util.Properties();
		config.put("StrictHostKeyChecking", "no");

		// not recommended for use on servers that have a key that can be
		// checked
		try {
			// set up jsch
			session = jsch.getSession(user.trim(), host.trim(), 22);
			session.setPassword(pass);

			// configure jsch
			session.setConfig("StrictHostKeyChecking", "no");

			// connect
			session.connect();

			// open up a command channel
			channel = session.openChannel("sftp");
			channel.connect();

			// convert channel to sftp channel
			sftp = (ChannelSftp) channel;

		} catch (JSchException e) {
			if (e.equals(ChannelSftp.SSH_FX_FAILURE)) {
				System.out.println("Already Exists");
			} else {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Disconnect
	 */
	public void disconnect() {
		// TODO disconnect and set session vars to null for garbage collection
		if (session != null) {
			session.disconnect();
			session = null;
		}

		if (sftp != null) {
			sftp.disconnect();
			sftp = null;
		}

		if (channel != null) {
			channel.disconnect();
			channel = null;
		}

	}

	/**
	 * Make directory
	 * 
	 * @param dir
	 */
	public void make(String dir) {

		try {

			// check for a directory
			sftp.mkdir(dir);

		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Upload Via File Path
	 */
	public void upload(String fpath) {
		try (FileInputStream fis = new FileInputStream(new File(fpath))) {
			byte[] b = new byte[(int) new File(fpath).length()];
			fis.read(b);
			this.set_bytes(b);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	/**
	 * Upload via bytes the path must be specified prior infname is the file
	 * name bytearr is the files bytes
	 * 
	 * @param bytearr
	 * @param infname
	 */
	public void uploadBytes(byte[] bytearr, String infname) {
		sftp_bytes(bytearr, infname);
	}

	private void sftp_bytes(byte[] bytearr, String infname) {
		// TODO FTP a byte array
		if (bytearr != null) {
			try {

				// if a path is set, set the current working directory
				if (path != null) {
					sftp.cd(path);
				}

				// create byte array input stream
				ByteArrayInputStream bis = new ByteArrayInputStream(bytearr);

				// put file on the system
				sftp.put(bis, infname);

			} catch (SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				throw new NullByteArrayException();
			} catch (NullByteArrayException e) {
				e.printStackTrace();
			}
		}
	}

	private void sftp_bytes(String filename) {
		// TODO FTP a byte array

		if (bytes != null) {
			try {

				// if a path is set, set the current working directory
				if (path != null) {
					sftp.cd(path);
				}

				// create byte array input stream
				ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

				// put file on the system
				sftp.put(bis, filename);

			} catch (SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				throw new NullByteArrayException();
			} catch (NullByteArrayException e) {
				e.printStackTrace();
			}
		}

	}

	private void sftp_no_bytes(String filename) {
		// TODO sftp an image to a server with user, password, host, and new
		// fpath

		// get File
		File f = new File(fpath);

		if (f.exists()) {
			try {

				// if a path is set, set the current working directory
				if (path != null) {
					sftp.cd(path);
				}

				// create File input Stream
				FileInputStream fis = new FileInputStream(f);

				// put file on the syste
				sftp.put(fis, filename);

			} catch (SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (java.io.FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				throw new FileNotFoundException();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Get a byte file from downloaded fpath Uses java.nio
	 * 
	 * @param fpath
	 * @return
	 */
	public byte[] downloadNIO(String fpath) {
		return doNIO(fpath);

	}

	/**
	 * Get a list of directories
	 * 
	 * @param dir
	 * @return directories
	 */
	public String ls(String dir) {
		String directories = null;
		Vector ls;
		try {
			ls = sftp.ls(dir);

			Iterator it = ls.iterator();

			while (it.hasNext()) {
				directories = (directories == null) ? it.next().toString()
						: directories + "\n" + it.next().toString();
			}

		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return directories;
	}

	private byte[] doNIO(String fpath) {
		// NIO character buffers for character files are faster than even the
		// byte arrays
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			if (path != null)
				sftp.cd(path);

			InputStream is = sftp.get(fpath);

			int b = -1;
			int i = 0;
			byte[] buf = new byte[1024];
			while (is.read(buf, 0, 1024) != -1) {
				baos.write(buf);
				i += 1024;
				buf = new byte[1024];
			}

		} catch (SftpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (baos.size() > 0) {
			return baos.toByteArray();
		}

		return null;
	}

	/**
	 * Change a directory
	 * 
	 * @param dir
	 */
	public void ChangeDirectory(String dir) {
		if (dir != null & sftp != null) {
			try {
				sftp.cd(dir);
			} catch (SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Perform non-byte download from prespecified {@path}
	 * 
	 * @param fpath
	 * @return
	 */
	public String download(String fpath) {
		return doDownload(fpath);
	}

	/**
	 * Download a Byte ArrayList using a specified file path from a prespecified
	 * {@path}
	 * 
	 * @param fpath
	 * @return
	 */
	public ArrayList<Byte> downloadBytes(String fpath) {
		return doDownloadBytes(fpath);
	}

	private ArrayList<Byte> doDownloadBytes(String fpath) {

		ArrayList<Byte> bytearr = new ArrayList<Byte>();
		byte[] bs = null;

		try {

			if (path != null)
				sftp.cd(path);

			InputStream is = sftp.get(fpath);

			byte[] buffer = new byte[1024];

			while (is.read(buffer) != -1) {
				for (int i = 0; i < buffer.length; i++) {
					if (buffer[i] == -1) {
						break;
					}
					bytearr.add(buffer[i]);
				}
			}

		} catch (SftpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bytearr;

	}

	private String doDownload(String fpath) {
		// NIO character buffers for character files are faster than even the
		// byte arrays
		String file = "";
		try {
			if (path != null)
				sftp.cd(fpath);

			InputStream is = sftp.get(fpath);

			InputStreamReader isr = new InputStreamReader(is);
			CharBuffer cbuff = CharBuffer.allocate(1024);
			boolean run = true;
			while (run) {
				isr.read(cbuff);

				cbuff.flip();
				String temp = cbuff.toString();
				if (temp.length() == 0) {
					run = false;
				}
				cbuff.flip();
				cbuff.clear();
				file += temp;
				temp = null;
			}

		} catch (SftpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return file;
	}

}

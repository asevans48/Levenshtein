package com.hygenics.html;

//for the database connection in test only

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.hygenics.exceptions.Invalid509Exception;

/**
 * Gets web pages for many technologies but not javascript (use ghost.py or
 * another program for this)
 * 
 * Technology Currently Supported:
 * 
 * html/xhtml ASP.net java server faces (.jsf and .html) java server pages (.jsp
 * and .html) limited flash base 64/gzipped data
 * 
 * Capabilities:
 * 
 * unzip gzipped data/base64 data rezip gzipped data/base64 data encode urls
 * authenticate passwords proxy support set cookies retrieve cookies cookie
 * manager support java SSL verification custom SSL verification settings for
 * timeouts get images via http/https
 *
 * Returns: String of html
 * 
 * Uses: java.nio for standard pulls java.net for everything else
 * 
 * For use when content-lengths are known or can be deciphered. Can also be used
 * to get the viewstate, html, and event validation code. Contains a base 64
 * decoder for decoding viewstates into useable information. (custom decoder
 * removes all unknown characters)
 * 
 * By: Andrew S. Evans
 * 
 */
public class html_grab {
	private boolean trust = false;
	private int timeout = 5000;
	private String keystore = null;
	private boolean set_keystore = false;
	private boolean hit_time;
	private String host;
	private String authority;
	private boolean proxy = false;
	private String cookies;
	private CookieManager mgr;
	private String[] headers;
	private String[] values;
	private String method;
	private String the_url;
	private String html;
	private String url_params;
	private static int[] toInt = new int[128];
	private final static char[] ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
			.toCharArray();

	public html_grab() {
		// TODO empty costructor
		mgr = new CookieManager();
		mgr.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
		CookieHandler.setDefault(mgr);
	}

	public html_grab(String[] inheaders, String[] invalues, String inurl) {
		// TODO actual constructor
		// initialize base 64 string
		// applied to overall values (adding i) to get appropriate Character
		for (int i = 0; i < ALPHABET.length; i++) {
			toInt[ALPHABET[i]] = i;
		}

		// the cookie manager
		// a precaution, works with HttpURLConnection to get any cookies
		mgr = new CookieManager();
		mgr.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
		CookieHandler.setDefault(mgr);

		values = invalues;
		headers = inheaders;
		method = "GET";
		the_url = inurl;
		url_params = null;
		host = null;
		authority = null;
	}

	public html_grab(String[] inheaders, String[] invalues, String inmethod,
			String inurl) {
		// TODO actual constructor
		// initialize base 64 int string
		// applied to overall values (adding i) to get appropriate Character
		for (int i = 0; i < ALPHABET.length; i++) {
			toInt[ALPHABET[i]] = i;
		}

		// the cookie manager
		// a precaution, works with HttpURLConnection to get any cookies
		mgr = new CookieManager();
		mgr.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
		CookieHandler.setDefault(mgr);

		values = invalues;
		headers = inheaders;
		method = inmethod;
		the_url = inurl;
		url_params = null;
		host = null;
		authority = null;
	}

	public html_grab(String[] inheaders, String[] invalues, String inmethod,
			String inurl, String inurl_params) {
		// TODO constructor for multiple values

		// initialize base 64 int string
		// applied to overall values (adding i) to get appropriate Character
		for (int i = 0; i < ALPHABET.length; i++) {
			toInt[ALPHABET[i]] = i;
		}

		// the cookie manager
		// a precaution, works with HttpURLConnection to get any cookies
		mgr = new CookieManager();
		mgr.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
		CookieHandler.setDefault(mgr);

		// initialize other values
		values = invalues;
		headers = inheaders;
		method = inmethod;
		the_url = inurl;
		url_params = inurl_params;
		host = null;
		authority = null;
	}

	public boolean isTrust() {
		return trust;
	}

	public void setTrust(boolean trust) {
		this.trust = trust;
	}

	public String[] getHeaders() {
		return headers;
	}

	public void setHeaders(String[] headers) {
		this.headers = headers;
	}

	public String[] getValues() {
		return values;
	}

	public void setValues(String[] values) {
		this.values = values;
	}

	public boolean getProxy() {
		return proxy;
	}

	public void setProxy(String inhost, String inport, boolean https,
			String user, String pass) {
		proxy(inhost, inport, https, user, pass);
		proxy = true;
	}

	public void setProxybySystem(boolean https, boolean secured) {
		systemproxy(https, secured);
		proxy = true;
	}

	private void systemproxy(boolean https, boolean secured) {
		if (https) {
			System.setProperty("https.proxyHost",
					System.getenv("httpsproxyhost"));
			System.setProperty("https.proxyPort",
					System.getenv("httpsproxyPort"));

			if (secured) {
				Authenticator authenticator = new Authenticator() {

					@Override
					public PasswordAuthentication getPasswordAuthentication() {
						return (new PasswordAuthentication(
								System.getenv("httpsproxyuser"), System.getenv(
										"httpsproxypass").toCharArray()));
					}
				};
				Authenticator.setDefault(authenticator);
			}
		} else {
			System.setProperty("https.proxyHost",
					System.getenv("httpproxyhost"));
			System.setProperty("https.proxyPort",
					System.getenv("httpproxyPort"));

			if (secured) {
				Authenticator authenticator = new Authenticator() {

					@Override
					public PasswordAuthentication getPasswordAuthentication() {
						return (new PasswordAuthentication(
								System.getenv("httpproxyuser"), System.getenv(
										"httpproxypass").toCharArray()));
					}
				};
				Authenticator.setDefault(authenticator);
			}
		}
	}

	private void proxy(String host, String port, boolean https, String user,
			String pass) {
		if (https) {
			System.setProperty("https.proxyHost", host);
			System.setProperty("https.proxyPort", port);

			if (user != null) {
				System.setProperty("https.proxyUser", user);
			}

			if (pass != null) {
				System.setProperty("https.proxyPassword", pass);
			}
		} else {
			System.setProperty("http.proxyHost", host);
			System.setProperty("http.proxyPort", port);

			if (user != null) {
				System.setProperty("http.proxyUser", user);
			}

			if (pass != null) {
				System.setProperty("http.proxyPassword", pass);
			}
		}
	}

	public String get_method() {
		return method;
	}

	public void reset_cookies() {
		// TODO call reset cookies
		new_cookies();
	}

	public void set_timeout(int millis) {
		// /TODO set the timeout
		timeout = millis;
	}

	public void set_cookies(String incookies) {
		// TODO set cookies
		cookies = incookies;
	}

	public String cookiegrab() {
		// TODO return cookies
		return cookies;
	}

	private void new_cookies() {
		// TODO delete cookies
		// the cookie manager
		// a precaution, works with HttpURLConnection to get any cookies

		if (mgr != null) {
			mgr.getCookieStore().removeAll();
		}

		mgr = new CookieManager();
		mgr.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
		CookieHandler.setDefault(mgr);
	}

	public void set_url_params(String inurl_params) {
		// TODO sets the url_params
		url_params = inurl_params;
	}

	public String get_secured() {
		// TODO Get a Self written secured page
		return get_manager();
	}

	public void set_host(String inhost) {
		// TODO set host
		host = inhost;
	}

	public void set_authority(String inauthority) {
		// TODO set authority
		authority = inauthority;
	}

	public String get_authority() {
		// TODO get authority
		return authority;
	}

	public String get_host() {
		// TODO set host
		return host;
	}

	public String get_cookies() {
		// TODO Method to call to get the page and cookies when a certificate is
		// self-signed by the site
		return getAspsessions();
	}

	public void set_html(String inhtml) {
		// TODO set the html
		html = inhtml;
	}

	public void set_url(String inurl) {
		// TODO set the url
		the_url = inurl;
	}

	public void set_method(String inmethod) {
		// TODO set the method type
		method = inmethod;
	}

	public void set_header_names(String[] innames) {
		// TODO add new header names array
		headers = innames;
	}

	public void set_values(String[] invalues) {
		// TODO add new header values array
		values = invalues;
	}

	public String get_redirect_path() {
		// TODO return a redirect
		return get_redirects();
	}

	public String get_url_params() {
		// TODO return url params
		return url_params;
	}

	public String get_html() {
		// TODO return the html content
		return html;
	}

	public String get_server_faces() {
		// TODO return a javax server faces viewstate
		if (html == null) {
			return null;
		} else {
			return (get_state(html,
					"(?<=id=\"javax.faces.ViewState\" value=\")[^'\\s\"]+"));
		}
	}

	public void encode_url_params(String[] innames, String[] invals) {
		// TODO encode the url parameters
		int length = innames.length;
		url_params = "";
		for (int i = 0; i < length; i++) {
			try {
				if (i == 0)
					url_params = URLEncoder.encode(innames[i], "UTF-8") + "="
							+ URLEncoder.encode(invals[i], "UTF-8");
				else
					url_params += "&" + URLEncoder.encode(innames[i], "UTF-8")
							+ "=" + URLEncoder.encode(invals[i], "UTF-8");

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

		}
	}

	public String getUrl() {
		return the_url;
	}

	public Boolean getSet_keystore() {
		return set_keystore;
	}

	public void setSet_keystore(Boolean inset) {
		set_keystore = inset;
	}

	public void setKeystore(String inpath) {
		keystore = inpath;
	}

	public String getKeystore() {
		return keystore;
	}

	public int get_param_size() {
		return url_params.length();
	}

	public String get_server_faces_decoded() {
		String ret_string = null;
		if (html == null) {
			return null;
		} else {
			try {
				ret_string = new String(decode(get_state(html,
						"(?<=id=\"javax.faces.ViewState\" value=\").*?\\/>")),
						"UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return ret_string;
	}

	public String get_decoded_viewstate() {
		String ret_string = null;

		if (html == null) {
			return null;
		}

		try {
			ret_string = new String(decode(get_state(html,
					"(?<=id=\"__VIEWSTATE\" value=\").*?\\/>")), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return ret_string;
	}

	public String get_previous_page() {
		if (html == null) {
			return null;
		}

		return get_state(html, "(?<=id=\"__PREVIOUSPAGE\" value=\").*?\\/>");
	}

	public String get_viewstate() {
		// TODO return the viewstate
		if (html == null) {
			return null;
		}

		return get_state(
				html,
				"(?mi)(?<=viewstate.).*?(?=\\|)|(?<=id=\"__VIEWSTATE\" value=\").*?\\/>|(?<=name=\"__VIEWSTATE\" value=\").*?\\/>");
	}

	public String get_event_validation() {

		// TODO return the event validation
		if (html == null) {
			return null;
		}

		return get_state(
				html,
				"(?mi)(?<=__EVENTVALIDATION.).*?(?=\\|)|(?<=id=\"__EVENTVALIDATION\" value=\").*?\\/>|(?<=name=\"__EVENTVALIDATION\" value=\").*?\\/>");
	}

	private String get_redirects() {
		// TODO Auto-generated method stub
		String redirect_path = null;
		if (html == null) {
			return null;
		} else {
			redirect_path = match_one_regex(html, "(?<=href=\").*?(?=\")");
		}
		return redirect_path;
	}

	private static byte[] decode(String s) {
		// TODO decode Base 64 into a byte array

		// find the ending positions number of characters
		int delta = s.endsWith("==") ? 2 : s.endsWith("=") ? 1 : 0;

		// set up the byte array which is going to be 75% of base 64 - the
		// difference
		byte[] buffer = new byte[s.length() * 3 / 4 - delta];

		// offset to replace the char with a byte
		int mask = 0xFF;

		// run down list of bytes and perform the transformation
		// letters are represented by two bytes corresponding to ascii but one
		// is generated per loop
		int index = 0;
		for (int i = 0; i < s.length(); i += 4) {

			// get the first two ints
			int c0 = toInt[s.charAt(i)];
			int c1 = toInt[s.charAt(i + 1)];

			// place the proper byte in the array with shifting first and second
			// bits
			buffer[index++] = (byte) (((c0 << 2) | (c1 >> 4)) & mask);

			// if the buffer is full return it
			if (index >= buffer.length) {
				return buffer;
			}

			// get the next character
			int c2 = toInt[s.charAt(i + 2)];

			// repeat the mask operations bit shift second <-- 4 OR 3rd shift
			// -->2 AND mask
			buffer[index++] = (byte) (((c1 << 4) | (c2 >> 2)) & mask);
			if (index >= buffer.length) {
				return buffer;
			}

			// perform the same with a 3rd char to create 4th bit
			int c3 = toInt[s.charAt(i + 3)];
			buffer[index++] = (byte) (((c2 << 6) | c3) & mask);
		}
		// return if reach here
		return buffer;
	}

	private String match_one_regex(String inhtml, String inpattern) {
		// TODO match a regex pattern
		String result = null;

		if (html != null & inpattern != null) {
			if (inhtml.trim().length() > 0) {
				Pattern p = Pattern.compile(inpattern, Pattern.MULTILINE);
				Matcher m = p.matcher(inhtml);

				while (m.find()) {
					result = m.group().trim();
				}
			}
		}

		return result;

	}

	private String get_state(String inhtml, String regex) {
		// TODO encapsulate the get state for security, returns the state
		// trimming whitespaces
		String state = null;
		// extracts the view state or event validation code
		if (inhtml != null & regex != null) {
			state = match_one_regex(inhtml, regex);
			if (state != null) {
				state = state.replace("\"", "").replace("/>", "").trim();
			}
		}

		return state;
	}

	/**
	 * Get ssl page
	 * 
	 * @return html
	 */
	public String get_SSL() {
		return get_ssl_page();
	}

	/**
	 * Get an ssl page
	 * 
	 * @return html
	 */
	private String get_ssl_page() {
		// TODO manually get SSL pages, Preferred method over Trust Manager
		// for use when the certificate is signed by a trusted authority
		// uses a basic HttpsUrlConnection

		try {

			if (set_keystore == true) {
				System.setProperty("javax.net.ssl.trustStore", keystore);
			}

			// url
			URL url = new URL(the_url);

			// connection
			HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
			conn.setConnectTimeout(timeout);
			conn.setReadTimeout(timeout);
			// set the ssl provider to be sun

			// set the request headers
			for (int i = 0; i < values.length; i++) {
				if (values[i] != null)
					conn.setRequestProperty(headers[i], values[i]);
			}

			// set up browser
			conn.setInstanceFollowRedirects(false);
			conn.setUseCaches(false);

			// set the method type
			if (method != null) {
				conn.setRequestMethod(method);

				if (method.compareTo("POST") == 0) {
					conn.setDoOutput(true);
					conn.setDoInput(true);
					conn.setInstanceFollowRedirects(false);

					if (url_params != null) {
						DataOutputStream wr = new DataOutputStream(
								conn.getOutputStream());
						wr.writeBytes(url_params);
						wr.flush();
						wr.close();
					}
				}

			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			html = "";
			int c = 0;

			while ((c = br.read()) != -1) {
				html += (char) c;
			}
			br.close();

			conn.getContent();
			cookies = format_cookies(mgr.getCookieStore().getCookies());
			conn.disconnect();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return html;
	}

	/**
	 * Gets a page using custom verification for self-signed certificates. Use
	 * Sparingly and wisely. Be Careful. This is a disclaimer.
	 *
	 * @return cookies
	 */
	private String get_manager() {
		// TODO Use a Trust Manager that accepts only a Certain Host Provider
		// for Self-signed certificates
		// All certificates must match the signing authority and host. There is
		// no leeway here!
		// Worst case scenarios should use this generic method. Otherwise try
		// adding the cert to a trust store

		try {
			// create the socket factory

			/*
			 * This pretty much overrides the trust manager and allows for the
			 * use of personal verification This code verifies the host,
			 * authority, and certificates manually and with the aid of a
			 * decryption tool. If the site is not trustworthy, do not use this
			 * method.
			 */

			// creates an all-trusting trustmanager which allows for
			// self-verification
			TrustManager[] tm = new TrustManager[] { new X509TrustManager() {
				public void checkClientTrusted(X509Certificate[] chain,
						String authType) throws CertificateException {
				}

				public void checkServerTrusted(X509Certificate[] chain,
						String authType) throws CertificateException {

				}

				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}

			} };

			// creates the SSL context for use in setting up the SSL handler
			final SSLContext ssl_context_handler = SSLContext
					.getInstance("SSL");

			// initializes
			ssl_context_handler
					.init(null, tm, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(ssl_context_handler
					.getSocketFactory());

			// sets the SSL Socket factory
			HttpsURLConnection.setDefaultSSLSocketFactory(ssl_context_handler
					.getSocketFactory());

			// url
			URL url = new URL(the_url);

			// connection

			HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
			conn.setConnectTimeout(timeout);
			conn.setReadTimeout(timeout);

			// set up browser
			conn.setInstanceFollowRedirects(false);
			conn.setUseCaches(false);

			for (int i = 0; i < values.length; i++) {
				if (values[i] != null)
					conn.setRequestProperty(headers[i], values[i]);
			}

			// set the method type
			if (method != null) {
				conn.setRequestMethod(method);

				// if the method is a POST, set the appropriate headers and
				// write out the POST parameters to the
				// output stream
				if (method.compareTo("POST") == 0) {
					conn.setDoOutput(true);
					conn.setDoInput(true);
					conn.setInstanceFollowRedirects(false);

					if (url_params != null) {
						// the output stream, nothing is coming in yet
						DataOutputStream wr = new DataOutputStream(
								conn.getOutputStream());
						wr.writeBytes(url_params);
						wr.flush();
						wr.close();
					}
				}

			}

			// create the input buffer which opens the connection for reading
			BufferedReader br = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));

			/*
			 * This is where the socket is vulnerable and bits should not be
			 * accepted for a MIME attack;etc. Verification can occur here sinc
			 * the certificate is obtained here. Do not proceed without a valid
			 * certificate from a trusted authority.
			 * 
			 * For non-Pentaho programs, it may be ok to place this code in a
			 * separate method although thatwould require passing the connection
			 * information between class methods in a public class.
			 */
			// loop through the certificates verifying that each is valid and
			// contains a trusted host and authority
			// provided by the user who should ensure that they are trustworthy

			java.security.cert.Certificate[] certs = conn
					.getServerCertificates();

			for (java.security.cert.Certificate cert : certs) {
				// handle verification for X509
				if (cert.getType().compareTo("X.509") == 0
						| cert.getType().compareTo("X509") == 0) {

					// certify from preset authorities after getting X509 typed
					// cert
					X509Certificate c = (X509Certificate) cert;

					// get the peer name
					// requires the existence of an OU
					String CN = conn.getPeerPrincipal().getName();

					int a, b = 0;

					a = CN.indexOf("CN=");
					b = CN.indexOf(",", a);

					if (b > a & a != -1) {
						CN = CN.substring((a + 3), b);
					}

					// get the authority
					// requires the existence of an OU
					String Issuer = c.getIssuerDN().getName();

					a = 0;
					b = 0;

					a = Issuer.indexOf("CN=");
					b = Issuer.indexOf(",", a);

					if (b > a & a != -1) {
						Issuer = Issuer.substring((a + 3), b);
					}

					// get the IssuerX500 Principal which should match the other
					// principal
					// requires the existence of an OU
					String IssuerX500 = c.getIssuerX500Principal().getName();
					a = 0;
					b = 0;

					a = IssuerX500.indexOf("CN=");
					b = IssuerX500.indexOf(",", a);

					if (b > a & a != -1) {
						IssuerX500 = IssuerX500.substring((a + 3), b);
					}

					// verify that the issuer, issuer principal, and peer
					// principal are valid
					// this can prevent MIME attacks for the most part but BE
					// CAREFUL

					// VERIFY HERE
					// System.out.println(CN+" "+Issuer);
					try {

						if (CN.compareTo(host) == 0
								& (Issuer.compareTo(authority) == 0 & IssuerX500
										.compareTo(authority) == 0)) {
							// if valid verify the certificate
							try {
								c.checkValidity();
							} catch (CertificateExpiredException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (CertificateNotYetValidException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else if (trust) {
							System.out.println("CN: " + CN + " Authority: "
									+ IssuerX500
									+ " Does Not Match Provided CN " + CN
									+ " and Provided Authority " + Issuer);
							System.out.println("Trust All is Set, Continuing");
						} else {
							// if the authority or host names do not match then
							// the Invalid Exception is thrown
							throw new Invalid509Exception();
						}
					} catch (Invalid509Exception e) {
						if (!trust) {
							e.printStackTrace();
							System.exit(-1);
						}
					}
				} else {
					// perform typical certificate verification
					// this is a back up used to catch misuse of the method
					// If this works, please try using the getSSL() method which
					// uses less code
					if (!trust) {
						try {
							cert.verify(cert.getPublicKey());
						} catch (InvalidKeyException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (CertificateException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (NoSuchProviderException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (SignatureException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}

			// personal verification for the certificate to handle self signed
			// X509 certificates
			// a host name must be provided
			// a certificate signer name must be provided
			// to be secure, these must be found elsewhere (to download the
			// certificate go to the web page in a browser and click the lock
			// icon or etc.)
			// This avoids a keystore and the slower SSL client under Pentaho

			/*
			 * Continue reading if the socket connection is secure
			 */

			conn.setConnectTimeout(timeout);
			conn.setReadTimeout(timeout);
			html = "";
			int c = 0;

			while ((c = br.read()) != -1) {
				html += (char) c;
			}

			conn.getContent();
			cookies = format_cookies(mgr.getCookieStore().getCookies());

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (SSLPeerUnverifiedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cookies;
	}

	/**
	 * Return whether the timeout was reached
	 * 
	 * @return
	 */
	public boolean get_hit_time() {
		// TODO return whether last grab hit the timeout or not
		return hit_time;
	}

	private String getAspsessions() {
		// TODO return the cookie string

		// try and catch block to get the pages
		try {

			/* get the starting page and its view state using a GET command */
			// this can be re-used since base 64 decoding is used by ASP .Net

			// set up the initial GET
			URL url = new URL(the_url);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setConnectTimeout(timeout);
			conn.setReadTimeout(timeout);

			// set up browser
			conn.setInstanceFollowRedirects(false);
			conn.setUseCaches(false);

			for (int i = 0; i < values.length; i++) {
				if (values[i] != null)
					conn.setRequestProperty(headers[i], values[i]);
			}

			// set the method type
			if (method != null) {
				conn.setRequestMethod(method);

				if (method.compareTo("POST") == 0) {
					conn.setDoOutput(true);
					conn.setDoInput(true);
					conn.setInstanceFollowRedirects(false);
				}

			}

			if (url_params != null) {
				DataOutputStream wr = new DataOutputStream(
						conn.getOutputStream());
				wr.writeBytes(url_params);
				wr.flush();
				wr.close();
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));

			html = "";
			int c = 0;

			while ((c = br.read()) != -1) {
				html += (char) c;
			}

			if (conn.getContentType() != null) {
				conn.getContent();
			} else {
				System.out.println(html);
			}

		} catch (MalformedURLException e) {// TODO failed to form url
			e.printStackTrace();
		} catch (IOException e) {
			// TODO failed to read site
			e.printStackTrace();
		}

		// return the cookie string
		return format_cookies(mgr.getCookieStore().getCookies());
	}

	private String format_cookies(List<HttpCookie> incookies) {

		String cookie_string = null;
		HttpCookie c = null;
		Iterator<HttpCookie> it = incookies.iterator();

		while (it.hasNext()) {
			c = it.next();
			try {
				// add the cookie to the list in the appropriate UTF-8 encoded
				// format
				// separated by a semi-colon
				if (cookie_string == null) {
					cookie_string = URLEncoder.encode(c.getName(), "UTF-8");
				} else {
					cookie_string += URLEncoder.encode(c.getName(), "UTF-8");
				}
				cookie_string += "=" + URLEncoder.encode(c.getValue(), "UTF-8")
						+ URLEncoder.encode(";", "UTF-8") + ";";

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}

		return cookie_string;
	}

	/**
	 * Encode and return a single url parameter
	 * 
	 * @param inurl
	 * @param inparameter
	 * @return encoded url
	 */
	public String url_encode(String inurl, String inparameter) {
		String url = inurl;

		try {
			if (url == null) {
				url = URLEncoder.encode(inparameter, "UTF-8");
			} else {
				url += URLEncoder.encode(inparameter, "UTF-8");
				;
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return url;
	}

	/**
	 * Return a random user agent including Mozilla 4+, Opera 9.80, and the
	 * Enigma Browser
	 * 
	 * @return useragent
	 */
	public String get_user_agent() {
		// TODO returns a random user agent string
		int random = (int) Math.floor(Math.random() * 9);
		// a string of user agents
		String[] agent = new String[10];
		agent[0] = "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:22.0) Gecko/20100101 Firefox/22.0";
		agent[5] = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; chromeframe/28.0.1500.71)";
		agent[6] = "Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3";
		agent[1] = "Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14";
		agent[3] = "Mozilla/5.0 (X11; Linux x86_64; rv:10.0.11) Gecko/20100101 conkeror/1.0pre (Debian-1.0~~pre+git120527-1)";
		agent[7] = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)";
		agent[2] = "Enigma Browser";
		agent[4] = "Mozilla/5.0 (X11; U; Linux; de-DE) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3)  Arora/0.8.0";
		agent[8] = "Mozilla/5.0 (compatible; MSIE 9.0; AOL 9.7; AOLBuild 4343.19; Windows NT 6.1; WOW64; Trident/5.0; FunWebProducts)";
		agent[9] = "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.1pre) Gecko/20090629 Vonkeror/1.0";
		return agent[random];
	}

}
package com.hygenics.parser;

import java.io.File;
import java.util.List;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;

/**
 * The Class that sends the PDA email of completed jobs and their record counts.
 * Attachments can be
 * 
 * @author aevans
 *
 */
public class Send {
	private static final Logger log = LoggerFactory.getLogger(MainApp.class);
	private JavaMailSender mailSender;
	private SimpleMailMessage message;

	private List<String> emails;
	private String fromEmails;

	private String subject;
	private String body;
	private String fpath;

	public Send() {

	}

	public JavaMailSender getMailSender() {
		return mailSender;
	}

	public void setMailSender(JavaMailSender mailSender) {
		this.mailSender = mailSender;
	}

	public List<String> getEmails() {
		return emails;
	}

	public void setEmails(List<String> emails) {
		this.emails = emails;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public String getFpath() {
		return fpath;
	}

	public void setFpath(String fpath) {
		this.fpath = fpath;
	}

	public SimpleMailMessage getMessage() {
		return message;
	}

	public void setMessage(SimpleMailMessage message) {
		this.message = message;
	}

	public String getFromEmails() {
		return fromEmails;
	}

	public void setFromEmails(String fromEmails) {
		this.fromEmails = fromEmails;
	}

	public void run() {

		try {
			log.info("Creating Message");
			MimeMessage message = mailSender.createMimeMessage();

			MimeMessageHelper helper = new MimeMessageHelper(message, true);

			InternetAddress addr = new InternetAddress(fromEmails);

			helper.setFrom(addr);

			for (String email : emails) {
				helper.addTo(new InternetAddress(email));
			}

			helper.setSubject(subject);
			helper.setText(body);

			if (fpath != null) {
				log.info("Attaching File");
				File f = new File(fpath);

				if (f.exists()) {
					helper.addAttachment(fpath, f);
				}
			}

			log.info("Sending Email");
			mailSender.send(message);

		} catch (AddressException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}

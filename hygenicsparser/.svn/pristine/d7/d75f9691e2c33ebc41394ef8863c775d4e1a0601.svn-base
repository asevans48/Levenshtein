package com.hygenics.parser;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

/**
 * Store an image loaded from file for quick access. Instantiated when needed,
 * the image can be blasted away. This is stored here to limit memory usage in a
 * string intensive program that already eats 750 mb of ram on average. It is
 * mainly for FaceMap.
 * 
 * @author asevans
 *
 */
public class ProxyImage {

	private static volatile ProxyImage pi;

	private String fpath;

	private BufferedImage bi;

	protected ProxyImage() {

	}

	/**
	 * Get singleton instance of proxy object to save memory use
	 * 
	 * @return
	 */
	public static ProxyImage getProxy() {
		synchronized (ProxyImage.class) {
			if (pi == null) {
				pi = new ProxyImage();
			}
		}
		return pi;
	}

	/**
	 * Set the Buffered Image from a given fpath
	 * 
	 * @param fpath
	 */
	public void setBi(String fpath) {
		this.fpath = fpath;
		synchronized (ProxyImage.class) {
			try {
				bi = ImageIO.read(new File(fpath));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * Get the BufferedImage from a given fpath
	 * 
	 * @return
	 */
	public BufferedImage getBI() {
		return bi;
	}

	/**
	 * Get the image Width
	 * 
	 * @return
	 */
	public int getWidth() {
		return bi.getWidth();
	}

	/**
	 * Get the Image Height
	 * 
	 * @return
	 */
	public int getHeight() {
		return bi.getHeight();
	}

	/**
	 * Return the color hash at the given point
	 * 
	 * @param x
	 * @param y
	 * @return
	 */
	public int getRgb(int x, int y) {
		return bi.getRGB(x, y);
	}

	/**
	 * Get the fpath
	 * 
	 * @return
	 */
	public String getFpath() {
		return fpath;
	}
}

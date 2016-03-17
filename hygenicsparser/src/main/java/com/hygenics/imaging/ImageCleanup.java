package com.hygenics.imaging;

//streams
import java.awt.Color;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.commons.imaging.ImageReadException;
import org.apache.commons.imaging.ImageWriteException;
import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.common.ImageMetadata;
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata;
import org.apache.commons.imaging.formats.jpeg.exif.ExifRewriter;
import org.apache.commons.imaging.formats.tiff.TiffImageMetadata;
import org.apache.commons.imaging.formats.tiff.constants.ExifTagConstants;
import org.apache.commons.imaging.formats.tiff.write.TiffOutputDirectory;
import org.apache.commons.imaging.formats.tiff.write.TiffOutputSet;

/***
 * This class is meant for sources that need a slight bit of touch up work and
 * to write meta-data in a less than public way despite its public heading.
 * 
 * Dos: Contains methods for simple cleanup
 * 
 * Doesnt's: This is not an image analysis tool. It only does some basic
 * reaveraging, contrast, sharpening, meta-data writing, and black and white
 * image creation.
 * 
 * @author aevans
 *
 */
public class ImageCleanup {

	/**
	 * Empty Constructor to set empty constructor access to public, limiting
	 * access but still visible instance variables
	 */
	public ImageCleanup() {

	}// DownloadImageCleanup

	/**
	 * Set the image type to jpg
	 */
	public void setImageType(String inurl, byte[] ibytes) {
		String imgType = null;
		String urltst = inurl;
		// get the image type from the url

		if (inurl != null) {
			urltst = inurl.toLowerCase();
		}

		// get the image type from the url which should contain the MIME type
		if (!urltst.toLowerCase().contains("jpg")
				| !urltst.toLowerCase().contains("jpeg")) {
			ByteArrayInputStream bis = new ByteArrayInputStream(ibytes);

			if (bis != null) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				// convert to jpeg for compression
				try {
					// use apache to read to a buffered image with certain
					// metadata and then convert to a jpg
					BufferedImage image = Imaging.getBufferedImage(bis);
					ImageIO.write(image, "jpg", bos);
					ibytes = bos.toByteArray();

				} catch (ImageReadException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		imgType = "jpg";
	}// setImageType

	public byte[] writeMetaData(String data, byte[] ibytes) {
		// write metadata based on the metadata columns list
		TiffOutputSet outset = null;
		BufferedImage bi;
		ImageMetadata metadata;
		ByteArrayOutputStream bos = null;
		try {

			// get the buffered image to write to
			bi = Imaging.getBufferedImage(ibytes);
			metadata = Imaging.getMetadata(ibytes);
			JpegImageMetadata jpegMetadata = (JpegImageMetadata) metadata;

			if (null != jpegMetadata) {
				// get the image exif data
				TiffImageMetadata exif = jpegMetadata.getExif();
				outset = exif.getOutputSet();
			}

			if (outset == null) {
				// get a new set (directory structured to write to)
				outset = new TiffOutputSet();
			}

			TiffOutputDirectory exdir = outset.getOrCreateExifDirectory();
			exdir.removeField(ExifTagConstants.EXIF_TAG_USER_COMMENT);

			exdir.add(ExifTagConstants.EXIF_TAG_USER_COMMENT, data.trim());

			bos = new ByteArrayOutputStream();
			ByteArrayInputStream bis = new ByteArrayInputStream(ibytes);

			ExifRewriter exrw = new ExifRewriter();

			// read to a byte stream
			exrw.updateExifMetadataLossy(bis, bos, outset);

			// read the input from the byte buffer
			ibytes = bos.toByteArray();
			bis.close();
			bos.close();

		} catch (ImageReadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ImageWriteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (bos != null) {
				try {
					bos.flush();
					bos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return ibytes;
	}// write meta data

	/**
	 * Convert to Bytes
	 * 
	 * @param imString
	 * @param magicNumber
	 * @return
	 */
	public byte[] convertToBytes(String imString, String magicNumber) {
		// check if dealing with an image and return the bytes if so
		if (imString.contains(magicNumber)) {
			return imString.getBytes();
		}
		return null;
	}// convertToBytes

	public byte[] reaverage(double factor, byte[] ibytes) {
		// tone red coloring
		BufferedImage color = BufferImage(ibytes);

		if (color != null) {
			BufferedImage averaged = new BufferedImage(color.getWidth(),
					color.getHeight(), BufferedImage.TYPE_INT_RGB);

			for (int i = 0; i < color.getWidth(); i++) {
				for (int j = 0; j < color.getHeight(); j++) {
					Color c = new Color(color.getRGB(i, j));

					averaged.setRGB(
							i,
							j,
							new Color((int) Math.round(c.getRed() / factor), c
									.getGreen(), c.getBlue()).getRGB());
				}
			}

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			// convert to jpeg for compression
			try {
				// use apache to read to a buffered image with certain metadata
				// and then convert to a jpg
				ImageIO.write(averaged, "jpg", bos);
				return ibytes = bos.toByteArray();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ibytes;

	}

	public byte[] setBlackandWhite(byte[] ibytes) {
		ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
		ColorConvertOp op = new ColorConvertOp(cs, null);
		BufferedImage image = op.filter(BufferImage(ibytes), null);

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		// convert to jpeg for compression
		try {
			// use apache to read to a buffered image with certain metadata and
			// then convert to a jpg
			ImageIO.write(image, "jpg", bos);
			return bos.toByteArray();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ibytes;
	}// setBlackandWhite

	BufferedImage BufferImage(byte[] ibytes) {
		// TODO return the image
		InputStream is = new ByteArrayInputStream(ibytes);
		BufferedImage img = null;

		try {
			img = ImageIO.read(is);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return img;
	}// BufferImage

	/**
	 * Private method that performs the sharpen
	 */
	public byte[] sharpen(byte[] ibytes, int weight, String format) {
		/*
		 * Kernel is |-1|-1|-1| |-1|weight|-1||-1|-1|-1|
		 */
		try {
			InputStream is = new ByteArrayInputStream(ibytes);
			BufferedImage proxyimage = ImageIO.read(is);
			// a secondary image for storing new outcomes
			BufferedImage image2 = new BufferedImage(proxyimage.getWidth(),
					proxyimage.getHeight(), BufferedImage.TYPE_BYTE_GRAY);

			// image width and height
			int width = proxyimage.getWidth();
			int height = proxyimage.getHeight();
			int r = 0;
			int g = 0;
			int b = 0;
			Color c = null;
			for (int x = 1; x < width - 1; x++) {
				for (int y = 1; y < height - 1; y++) {
					// sharpen the image using the kernel (center is c5)
					Color c00 = new Color(proxyimage.getRGB(x - 1, y - 1));
					Color c01 = new Color(proxyimage.getRGB(x - 1, y));
					Color c02 = new Color(proxyimage.getRGB(x - 1, y + 1));
					Color c10 = new Color(proxyimage.getRGB(x, y - 1));
					Color c11 = new Color(proxyimage.getRGB(x, y));
					Color c12 = new Color(proxyimage.getRGB(x, y + 1));
					Color c20 = new Color(proxyimage.getRGB(x + 1, y - 1));
					Color c21 = new Color(proxyimage.getRGB(x + 1, y));
					Color c22 = new Color(proxyimage.getRGB(x + 1, y + 1));

					// apply the kernel for r
					r = -c00.getRed() - c01.getRed() - c02.getRed()
							- c10.getRed() + (weight * c11.getRed())
							- c12.getRed() - c20.getRed() - c21.getRed()
							- c22.getRed();

					// apply the kernel for g
					g = c00.getGreen() - c01.getGreen() - c02.getGreen()
							- c10.getGreen() + (weight * c11.getGreen())
							- c12.getGreen() - c20.getGreen() - c21.getGreen()
							- c22.getGreen();

					// apply the transformation for b
					b = c00.getBlue() - c01.getBlue() - c02.getBlue()
							- c10.getBlue() + (weight * c11.getBlue())
							- c12.getBlue() - c20.getBlue() - c21.getBlue()
							- c22.getBlue();

					// set the new rgb values
					r = Math.min(255, Math.max(0, r));
					g = Math.min(255, Math.max(0, g));
					b = Math.min(255, Math.max(0, b));

					c = new Color(r, g, b);

					// set the new mask colors in the new image
					image2.setRGB(x, y, c.getRGB());

				}
			}

			// add the new values back to the original image
			Color cmask = null;
			Color corig = null;
			for (int x = 1; x < width - 1; x++) {
				for (int y = 1; y < height - 1; y++) {
					// get the 2 colors
					cmask = new Color(image2.getRGB(x, y));
					corig = new Color(proxyimage.getRGB(x, y));

					// add the new values
					r = cmask.getRed() + corig.getRed();
					g = cmask.getGreen() + corig.getGreen();
					b = cmask.getBlue() + corig.getBlue();

					// set the new rgb values
					r = Math.min(255, Math.max(0, r));
					g = Math.min(255, Math.max(0, g));
					b = Math.min(255, Math.max(0, b));

					proxyimage.setRGB(x, y, new Color(r, g, b).getRGB());
				}
			}

			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				ImageIO.write(proxyimage, format, baos);
				ibytes = baos.toByteArray();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ibytes;
	}
}

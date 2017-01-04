package com.salmon.dataload.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

/**
 * Encapsulates a Java properties file.  Allows the application to store values in the 
 * properties file.
 * <p>
 * Use {@link #getValue(String, String)} or {@link #getProperties()} to open the file
 * and {@link #save()} to save and close the file.    
 * @author David Nice
 *
 */
public class PropertyValues {

	/** Property file name.  
	 */
	private String filename;
	
	/** Current properties hash.  
	 */
	private Properties properties;

	
	/**
	 * Gets all the properties within the property file.  
	 * @return All properties
	 */
	public Properties getProperties() {
		if (properties == null) {
			properties = new Properties();
			File propertiesFile = new File(getFilename());
			InputStream in = null;
			
			if (propertiesFile.isFile()) {
				try {
					in = new BufferedInputStream(new FileInputStream(getFilename()));
					properties.load(in);
				} 
				catch (FileNotFoundException e) {
					e.printStackTrace();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
				finally {
					try {
						if (in != null) in.close();
					} catch (IOException e) {
						// Do nothing
					}
				}
			}
		}
			
		return properties;
	}

	/**
	 * Sets a property value
	 * @param keyName Key name
	 * @param value New value
	 */
	public void setValue(String keyName, String value) {
		getProperties().setProperty(keyName, value);
		save();
	}

	/**
	 * Saves the properties file.  
	 */
	public void save() {
		OutputStream out = null;

		try {
			out = new BufferedOutputStream(new java.io.FileOutputStream(getFilename()));
			getProperties().store(out, null);
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (out != null) out.close();
			} catch (IOException e) {
				// Do nothing
			}
		}
	}
	
	/**
	 * Gets value from properties file.  
	 * @param keyName Key name
	 * @param defaultValue Default value
	 * @return Property value
	 */
	public String getValue(String keyName, String defaultValue) {
		return getProperties().getProperty(keyName, defaultValue);
	}
	
	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

}

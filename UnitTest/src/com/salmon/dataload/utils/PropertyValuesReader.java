package com.salmon.dataload.utils;


/**
 * Extended class from PropertyValues just to make it as singleton.  
 * @author Anil Maindola
 *
 */
public class PropertyValuesReader extends PropertyValues {

	private static PropertyValuesReader singleInstance = null;

	/**
	 * Marking default constructor private 
	 * to avoid direct instantiation. 
	 */
	private PropertyValuesReader() {    
		
	}  
	
	/**
	 * Get instance for class SimpleSingleton 
	 * @return propertyValueReader
	 */
	public static PropertyValuesReader getInstance() {             
		if (null == singleInstance) {
			singleInstance = new PropertyValuesReader();        
		}
		
		return singleInstance;    
	}
}

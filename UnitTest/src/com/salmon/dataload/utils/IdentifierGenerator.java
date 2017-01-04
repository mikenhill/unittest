/*
 * Created on 19-Jul-05
 */
package com.salmon.dataload.utils;

/**
 * This is a utility class that provides methods that convert identifiers into
 * a more compact form.  This is done by either removing all non-letter or digit 
 * characters from a string or by generating a CRC value.    
 * <p />
 * It is up to the calling method to ensure the uniqueness of the supplied string.  
 * @author David Nice
 */
public final class IdentifierGenerator {
	
	/** Use an open source utility to generate the CRC.  
	 */
	static CRC32 crc = new CRC32();

	/**
	 * Ensure that this method cannot be instantiated.  
	 */
	private IdentifierGenerator() {
	}

	/**
	 * In effect, this method removes all characters that are not letters or digits 
	 * from a string.  
	 * @param string Input string
	 * @return Identifier String
	 */
	public static String createIdentifier(String string) {
		StringBuffer buffer = new StringBuffer();
		
		for (int index = 0 ; index < string.length() ; index++) 
			if (Character.isLetterOrDigit(string.charAt(index)))
				buffer.append(string.charAt(index));

		return buffer.toString();
	}
	
	/**
	 * Creates a CRC from a string.  Unique strings should generate unique CRC values.  
	 * @param string Input string
	 * @return CRC string
	 */
	public static String createCRC(String string) {
		return Integer.toString(crc.crc32(string));
	}
	
	/**
	 * A combination of {@link IdentifierGenerator#createCRC(String)} 
	 * and {@link IdentifierGenerator#createIdentifier(String).  
	 * @param string Input string
	 * @return CRC string
	 */
	public static String createIdentifierCRC(String string) {
		int result = crc.crc32(createIdentifier(string).toLowerCase());
		return Integer.toHexString(result);
	}
}

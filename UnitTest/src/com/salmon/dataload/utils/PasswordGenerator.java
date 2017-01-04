package com.salmon.dataload.utils;

import org.apache.commons.lang.RandomStringUtils;

import com.ibm.commerce.util.nc_crypt;
import com.ibm.commerce.util.nc_hash;

/**
 * Utility class that encrypts a password so that it is compatible with IBM WebSphere Commerce.  
 * The encrypted password is based on a password string, salt and a merchant key.  The merchant
 * key is supplied to the constructor, which effectively binds the encrypted password to an
 * instance of WCS.   
 * @author David Nice
 */
public class PasswordGenerator {

    /** Length of the salt string.   
     */
    private static final int SALT_STRING_LENGTH = 12;
    
	/** Merchant key.  
     */
    private String merchantKey = null;

	/**
	 * Mandatory constructor.  
	 * @param merchantKey Merchant key
	 */
	public PasswordGenerator(String merchantKey) {
		this.merchantKey = merchantKey;
	}

	/**
	 * Default constructor that uses a default merchant key.  
	 */
	public PasswordGenerator() {
	}
	
	/**
	 * @param strInput ASCII version of the encrypted password
	 * @return Hexadecimal version of the encrypted password
	 */
	public String getHexFormat(String strInput) {
        return nc_crypt.bytesToHexString(nc_crypt.stringToBytes(strInput));
    }

	/**
	 * @param password Password string
	 * @param salt Random set of characters.  
	 * @return ASCII version of the encrypted password
	 */
	public String encryptPassword(String password, String salt) {
		String strPasswordHash = nc_hash.hash(salt.trim() + password.trim()).trim();
        return nc_crypt.encrypt(strPasswordHash.trim(), merchantKey.trim()).trim();
	}
	
	/**
	 * Decrypts a string that has been encrypted using the Commerce encryption facilities.  
	 * If the supplied string has not been encrypted then the original string will be 
	 * returned.   
	 * @param string String to decrypted
	 * @return Decrypted string
	 */
	public String decryptString(String string) {
		String decryptedString = nc_crypt.decrypt(string, merchantKey != null ? merchantKey.trim() : null);
		
		return decryptedString != null ? decryptedString : string;
	}
	
	/**
	 * Generates a random set of alphanumeric characters.  
	 * @return A string that can be used as a new salt value.  
	 */
	public String createSalt() {
		return RandomStringUtils.randomAlphanumeric(SALT_STRING_LENGTH);
	}
	
}

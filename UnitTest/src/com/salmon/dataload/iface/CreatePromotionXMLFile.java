package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_TARGET_FILE;



import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.helper.PromotionEtlHelper;
import com.salmon.dataload.helper.TableName;

/**
 * This class fetches the data from XINT_PROMOTIONDATA table and also creates the promotion.xml in the promotion utility folder.
 * 
 * @author Keshav Chawla
 * @revision : 1.0
 * @Date : 27 July 2014
 */
public final class CreatePromotionXMLFile {
    
    private static final int NUM_ARGUMENTS = 5;
    private static final int SOURCE_DIR    = 0;
    private static final int JDBC_DRIVER   = 1;
    private static final int DB_URL        = 2 ;
    private static final int DB_USERNAME   = 3;
    private static final int DB_PASSWORD   = 4;   
    private String jdbcDriver;
    private String dbUrl;
    private String dbUserName;
    private String dbPassword;
    
    private static final String CLASSNAME = CreatePromotionXMLFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    
    private static final String FILE_NAME="/promotion.xml";
    
    PromotionEtlHelper promotionEtlHelper = null;
    
    public PromotionEtlHelper getPromotionEtlHelper()
    throws ClassNotFoundException, SQLException {
        if (promotionEtlHelper == null) {
            promotionEtlHelper = new PromotionEtlHelper(jdbcDriver,  dbUrl,  dbUserName,  dbPassword);
        }
        return promotionEtlHelper;
    }

    public void setPromotionEtlHelper(PromotionEtlHelper promotionEtlHelper) {
        this.promotionEtlHelper = promotionEtlHelper;
    }    

    /**
     * @param sourceDirectory
     * @param jdbcDriver
     * @param dbURL
     * @param dbUserName
     * @param dbPassword
     * @throws SQLException
     */
    public static void main(final String[] args) throws SQLException {
        String methodName = "main";
        
        if (args.length == NUM_ARGUMENTS 
            && args[JDBC_DRIVER].length() > 0 
            && args[DB_URL].length() > 0 
            && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0) {
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Process to create a promotion xml ");
            
            CreatePromotionXMLFile createPromotion = new CreatePromotionXMLFile();
            createPromotion.run(args[SOURCE_DIR], 
                                args[JDBC_DRIVER], 
                                args[DB_URL], 
                                args[DB_USERNAME], 
                                args[DB_PASSWORD]);            
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to CreatePromotionXMLFile");
            System.exit(INVALID_ARGUMENTS);
        }                    
    }
        
    /**
     * @param sourceDirectory
     * @param jdbcDriver
     * @param dbURL
     * @param dbUserName
     * @param dbPassword
     * @throws SQLException
     */
    public void run(String sourceDirectory,String jdbcDriver, String dbURL, String dbUserName, String dbPassword) throws SQLException {      
        String methodName = "run";
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Creating promotion XML"); 
              
        try{                              
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Creating " +sourceDirectory + FILE_NAME);
            File file = new File(sourceDirectory + FILE_NAME);
            FileOutputStream fop = new FileOutputStream(file) ; 
            
            this.jdbcDriver = jdbcDriver;
            this.dbUrl = dbURL;
            this.dbUserName = dbUserName;
            this.dbPassword = dbPassword;
            String[] promotionXML = getPromotionEtlHelper().createPromotionXML();    
            String promotionXMLString = promotionXML[0];

            generatePromotionXML(promotionXMLString, file, fop);

            
            getPromotionEtlHelper().commit();
            getPromotionEtlHelper().close();
            

        } catch (IOException e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "FileNotFoundException Exception"+e.getMessage());            
            System.exit(ERROR_WRITING_TARGET_FILE); 
        } catch (ClassNotFoundException e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "ClassNotFoundException Exception"+e.getMessage());            
            System.exit(ERROR_WRITING_TARGET_FILE); 
        }
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Exiting");      
    }   
                  
    /**
     * This method is to generate the promotion xml and store it in the specified directory.
     * @param xml
     * @param sourceDir
     */
    private void generatePromotionXML(String promoXML,File file, FileOutputStream fop) throws IOException {      
 
        // if file doesn't exists, then create it
        file.createNewFile();
        // get the content in bytes
        byte[] promoXMLInBytes = promoXML.getBytes();     
        fop.write(promoXMLInBytes);
        fop.flush();
        fop.close();                             

    }              
}
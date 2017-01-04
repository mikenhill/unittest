package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.logging.Level;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.helper.PromotionEtlHelper;


public class InsertParentProduct {
    
    private static final int NUM_ARGUMENTS = 4;
    private static final int JDBC_DRIVER = 0;
    private static final int DB_URL      = 1;
    private static final int DB_USERNAME = 2;
    private static final int DB_PASSWORD = 3;    
    
    private static final String CLASSNAME = InsertParentProduct.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    
    
    public static void main(final String[] args) {
        String methodName = "main";
        
        if (args.length == NUM_ARGUMENTS 
                && args[JDBC_DRIVER].length() > 0 
                && args[DB_URL].length() > 0 
                && args[DB_USERNAME].length() > 0 
                && args[DB_PASSWORD].length() > 0 )                
        {
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Process to create a parent product ");
            InsertParentProduct insertProduct = new InsertParentProduct();
            insertProduct.run(args[JDBC_DRIVER], 
                              args[DB_URL], 
                              args[DB_USERNAME], 
                              args[DB_PASSWORD]);            
        }else{
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to InsertParentProduct");
            System.exit(INVALID_ARGUMENTS);
        }
    }
    
    public void run(String jdbcDriver, String dbURL, String dbUserName, String dbPassword) { 
        String methodName = "run";
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Inserting parent product ");   
        
        try {
            
            MonitiseEtlHelper  monitiseEtlHelper =  new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            monitiseEtlHelper.createParentProduct();        
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();      
            
        } catch (SQLException sql){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        } catch (ClassNotFoundException cnfe){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "ClassNotFoundException Exception"+cnfe.getMessage());
            logStackTrace(cnfe, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Exiting");
      
    }
    
    private static void logStackTrace(final Throwable aThrowable, final String methodName) {
        Writer result = new StringWriter();
        aThrowable.printStackTrace(new PrintWriter(result));
        LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, result.toString());
    }

}

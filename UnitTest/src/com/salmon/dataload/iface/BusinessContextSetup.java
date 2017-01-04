package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.logging.Level;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.utils.FileUtilities;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * This Class reads the content provider code from xint_productdata-extract.csv and retrieves the store and calatog id 
 * and set it in the  Business context file wc-dataload-env-cp.xml. 
 * 
 * 
 * @author Paul Chacko
 * @revision : 1.0
 * @Date : 12 June 2014
 */

public class BusinessContextSetup {
    
    private static final int NUM_ARGUMENTS    = 8;
    private static final int JDBC_DRIVER      = 0;
    private static final int DB_URL           = 1;
    private static final int DB_USERNAME      = 2;
    private static final int DB_PASSWORD      = 3;
    private static final int SOURCEFILEPATH   = 4;
    private static final int SOURCEFILENAME   = 5;
    private static final int ENVSETUPFILEPATH = 6;
    private static final int ENVSETUPFILENAME = 7;
    
    static final String DELIMITER = ",";
    private static final String DEFAULTCAS = "Extended Sites Catalog Asset Store";
    
    private static final String CLASSNAME = BusinessContextSetup.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    
    StringBuffer sb = new StringBuffer();            
    
    
    public static void main(final String[] args) {
        String methodName = "main";
        
        if (args.length == NUM_ARGUMENTS  
                && args[JDBC_DRIVER].length() > 0 
                && args[DB_URL].length() > 0 
                && args[DB_USERNAME].length() > 0 
                && args[DB_PASSWORD].length() > 0 
                && args[SOURCEFILEPATH].length() > 0 
                && args[SOURCEFILENAME].length() > 0 
                && args[ENVSETUPFILEPATH].length() > 0 
                && args[ENVSETUPFILENAME].length() > 0) {
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Retrieving the Store and catalog id ");
            
            BusinessContextSetup setUp = new BusinessContextSetup();
            setUp.run(args[JDBC_DRIVER], 
                      args[DB_URL], 
                      args[DB_USERNAME], 
                      args[DB_PASSWORD], 
                      args[SOURCEFILEPATH], 
                      args[SOURCEFILENAME], 
                      args[ENVSETUPFILEPATH], 
                      args[ENVSETUPFILENAME] );
            
        } else {
            
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to BusinessContextSetup");
            System.exit(INVALID_ARGUMENTS);
        }
    }
    
    
    public void run(String jdbcDriver, String dbURL, String dbUserName, String dbPassword, String sourceFilePath, String sourceFileName, String envFilePath, String envFileName) {
        String methodName = "run";      
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Setting up BusinessContext: "); 
        
        String cpCode            = null;
        String catalogIdentifier = null;
        
        try{            
            cpCode = getCpCode(sourceFilePath, sourceFileName);
            
            cpCode = cpCode.replaceAll("\\\"", "");
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Content Provider code:" + cpCode ); 
            
            catalogIdentifier = getCatalogIdentifier(cpCode, jdbcDriver, dbURL, dbUserName, dbPassword);
            
            sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            sb.append("<!DOCTYPE DataLoadEnvConfiguration [\n");
            sb.append("<!ENTITY % definitions SYSTEM \"file:store-definitions.dtd\">\n");
            sb.append("%definitions;\n");
            sb.append("]>\n");
            sb.append("<!--\n");
            sb.append("=================================================================\n");
            sb.append("  Licensed Materials - Property of IBM                           \n");
            sb.append("                                                                 \n");
            sb.append("  WebSphere Commerce\n");
            sb.append("                                                                  \n");
            sb.append("  (C) Copyright IBM Corp. 2009 All Rights Reserved.\n");
            sb.append("                                                   \n");
            sb.append("  US Government Users Restricted Rights - Use, duplication or\n");
            sb.append("  disclosure restricted by GSA ADP Schedule Contract with\n");
            sb.append("  IBM Corp. \n");
            sb.append(" =================================================================\n");
            sb.append("-->\n");
            sb.append("<_config:DataLoadEnvConfiguration\n");
            sb.append("    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
            sb.append("    xsi:schemaLocation=\"http://www.ibm.com/xmlns/prod/commerce/foundation/config xsd/wc-dataload-env.xsd\" \n");
            sb.append("    xmlns:_config=\"http://www.ibm.com/xmlns/prod/commerce/foundation/config\">\n");
            sb.append("                                                                                \n");    
    
            sb.append("    <_config:BusinessContext storeIdentifier=\""+cpCode+"\" catalogIdentifier=\""+catalogIdentifier+"\" \n");
            sb.append("        languageId=\"&LANGUAGE_ID;\" currency=\"&DATALOAD_DEFAULT_CURRENCY;\">\n");
            sb.append("    </_config:BusinessContext>   \n");   
                
            sb.append("    <_config:Database type=\"&DATALOAD_DB_TYPE;\" name=\"&DATALOAD_DB_NAME;\" user=\"&DATALOAD_DB_USER;\" password=\"&DATALOAD_DB_PASSWORD;\" \n");
            sb.append("            server=\"&DATALOAD_DB_SERVER;\" port=\"&DATALOAD_DB_PORT;\" schema=\"&DATALOAD_DB_SCHEMA;\" />\n");
            sb.append("                                                                                                   \n");
            sb.append("    <_config:IDResolver className=\"com.ibm.commerce.foundation.dataload.idresolve.IDResolverImpl\" />\n");
            sb.append("                                                                                                     \n");     
            sb.append("    <!-- \n");
            sb.append("    <_config:Database type=\"&DATALOAD_DB_TYPE;\" name=\"&DATALOAD_DB_NAME;\" user=\"&DATALOAD_DB_USER;\" password=\"&DATALOAD_DB_PASSWORD;\" \n");
            sb.append("            server=\"&DATALOAD_DB_SERVER;\" port=\"&DATALOAD_DB_PORT;\" schema=\"&DATALOAD_DB_SCHEMA;\" driverType=\"&DATALOAD_DB_DRIVER_TYPE;\"/>\n");
            sb.append("     -->\n");
            sb.append("                                                                                                 \n");     
            sb.append("    <_config:DataWriter className=\"com.ibm.commerce.foundation.dataload.datawriter.JDBCDataWriter\" />\n");
            sb.append("                                                                                                       \n");
            sb.append("</_config:DataLoadEnvConfiguration>\n");
            
            writeOutputFile(envFilePath, envFileName);
       
        } catch(Exception e){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Exception"+e.getMessage());
            logStackTrace(e, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Exiting");
      
    }
    
    private String getCpCode (final String sourceFilePath, final String sourceFileName) {
        String methodName = "getCpCode";
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "sourceFilePath:" + sourceFilePath
                + " sourceFileName:" + sourceFileName);
        
        String cpCode  = DEFAULTCAS;
        String strLine = null;
        
        try {
        
            File            sourceFile = new File(sourceFilePath, sourceFileName);  
            FileInputStream fis        = new FileInputStream(sourceFile);
            long totalLinesCount       = FileUtilities.getTotalLinesCount(sourceFile);
            
            InputStream       cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in          = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader    br          = new BufferedReader(in);
            
            if (totalLinesCount > 1) {
                for (int i = 0; i < 2; i++) {
                    strLine = br.readLine();
                    if ( i > 0) {   
                        cpCode = strLine.split(DELIMITER)[0];
                    }
                }
            } 

            br.close();
            in.close();
            fis.close();
       
        } catch(Exception e){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Exception"+e.getMessage());
            logStackTrace(e, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }
        
        return cpCode;
    }
    
    private String getCatalogIdentifier (final String cpCode, final String jdbcDriver, final String dbURL, final String dbUserName, final String dbPassword) {
        String methodName = "getCatalogIdentifier";
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "cpCode:" + cpCode); 
        String catalogIdentifier = null;
        
        try {
            MonitiseEtlHelper  monitiseEtlHelper = new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            catalogIdentifier = monitiseEtlHelper.getCatalogIdentifier(cpCode);
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Content Provider Catalog Identifier:" + catalogIdentifier );         
            
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();       
            
        } catch(SQLException sql){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, "run");
            System.exit(NO_SOURCE_FILE_FOUND);        
        } catch(ClassNotFoundException cnfe){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "ClassNotFoundException Exception"+cnfe.getMessage());
            logStackTrace(cnfe, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }
        
        return catalogIdentifier;
    }
    
    private void writeOutputFile (final String envFilePath, final String envFileName) {   
        String methodName = "writeOutputFile";
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Start"); 
        
        try {
            File outputFile = new File(envFilePath, envFileName);
            FileWriter fw = new FileWriter(outputFile);
            fw.write(sb.toString());
            
            fw.close();
       
        } catch(Exception e){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Exception"+e.getMessage());
            logStackTrace(e, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }            
    }
    
    private static void logStackTrace(final Throwable aThrowable, final String methodName) {
        Writer result = new StringWriter();
        aThrowable.printStackTrace(new PrintWriter(result));
        LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, result.toString());
    }
}

package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.helper.TableName;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * Generates the log file from the any etl source file. 
 * 
 * @author Pranava Mishra
 * @revision : 1.0
 * @Date : 28 May 2014
 */

public final class GenerateLogsFile {

    private static final String CLASSNAME = GenerateLogsFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    static final String DELIMITER = "\\|";
    private static final String FOOTER = "FTRAIL";
    private static final String LOG_FILE_EXTENSION = ".log";
    private static final String ARCHIVE_FOLDER = "archive";
    
    private static final int SOURCE_FILE_PATH    = 0;
    private static final int SOURCE_FILE_PREFIX  = 1;
    private static final int VALIDATED_FILE_NAME = 2;
    private static final int TABLE_NAME          = 3;
    private static final int JDBC_DRIVER         = 4;
    private static final int DB_URL              = 5;
    private static final int DB_USERNAME         = 6;
    private static final int DB_PASSWORD         = 7;
    
    private static final String LOG_FOLDER = "logs";    
    
    private static final int NUM_ARGUMENTS = 8; 
    
    private final String sourceFilePath;
    private final String sourceFilePrefix;
    private final String validatedFileName;


    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param logFilePath - log file path
     * 
     */
    public GenerateLogsFile(final String sourceFilePath, final String sourceFilePrefix, final String validatedFileName) {
        this.sourceFilePath    = sourceFilePath;
        this.sourceFilePrefix  = sourceFilePrefix;
        this.validatedFileName = validatedFileName;    
    } 
  
    
    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
     public void generateLogFile(String tableName,String jdbcDriver, String dbURL, String dbUserName, String dbPassword ) {
        String methodName = "generateLogFile"; 
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "tableName:" + tableName);         
         
        String sourceArchiveFileName = getSourceArchiveFileName(validatedFileName); 
        
        try {            
            String inputFilePath  = sourceFilePath + File.separator + ARCHIVE_FOLDER + File.separator + sourceArchiveFileName;
            String outputFilePath = sourceFilePath + File.separator + LOG_FOLDER     + File.separator + sourceArchiveFileName + LOG_FILE_EXTENSION;
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " sourceArchiveFileName:" + sourceArchiveFileName 
                    + " inputFilePath:" + inputFilePath + " outputFilePath:" + outputFilePath);
            
            File               logFile = new File(outputFilePath);
            FileOutputStream   fos     = new FileOutputStream(logFile);
            OutputStreamWriter osw     = new OutputStreamWriter(fos, "UTF-8");
            BufferedWriter     logBw   = new BufferedWriter(osw); 
            
            MonitiseEtlHelper  monitiseEtlHelper = new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            
            List<String> logData = new ArrayList<String>();
            
            if (tableName.equals(TableName.XINT_STOCKDATA.name())) {
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_STOCKDATA,    sourceArchiveFileName);
            } else if (tableName.equals(TableName.XINT_PRICEDATA.name())){
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_PRICEDATA,    sourceArchiveFileName);
            } else if (tableName.equals(TableName.XINT_STORESDATA.name())){
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_STORESDATA,   sourceArchiveFileName);
            } else if (tableName.equals(TableName.XINT_MASDATA.name())){
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_MASDATA,      sourceArchiveFileName);
            } else if (tableName.equals(TableName.XINT_OFFERSDATA.name())){
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_OFFERSDATA,   sourceArchiveFileName);
            } else if (tableName.equals(TableName.XINT_LANGDATA.name())){
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_LANGDATA,     sourceArchiveFileName);
            } else if (tableName.equals(TableName.XINT_ATTRIBUTEDATA.name())){
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_ATTRIBUTEDATA,sourceArchiveFileName);
            } else if (tableName.equals(TableName.XINT_PROMOTIONDATA.name())){
                logData=monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_PROMOTIONDATA,sourceArchiveFileName);
            }
            
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();            
            
            // Get header
            if (logData!=null) {
                for (int count = 0; count<logData.size(); count++) {
                    logBw.write(logData.get(count));
                    logBw.newLine();                                                                             
                }
            }
            logBw.close();
          
        } catch (Exception e) {
            logStackTrace(e, methodName);
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);
    }
    
    
    
    /**
     * Two arguments must be required, the source product attributes file name, and the target product attributes file name.
     * 
     * @param args - input arguments
     */
    public static void main(final String[] args) {
        String methodName = "main";
        if (args.length == NUM_ARGUMENTS 
            && args[SOURCE_FILE_PATH].length() > 0 
            && args[SOURCE_FILE_PREFIX].length() > 0
            && args[VALIDATED_FILE_NAME].length() > 0 
            && args[TABLE_NAME].length() > 0 
            && args[JDBC_DRIVER].length() > 0 
            && args[DB_URL].length() > 0 
            && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0 
        ) {
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating file: " + args[SOURCE_FILE_PATH]);
            GenerateLogsFile log = new GenerateLogsFile(args[SOURCE_FILE_PATH], 
                                                        args[SOURCE_FILE_PREFIX], 
                                                        args[VALIDATED_FILE_NAME]);
            log.generateLogFile(args[TABLE_NAME],
                                args[JDBC_DRIVER], 
                                args[DB_URL], 
                                args[DB_USERNAME], 
                                args[DB_PASSWORD]);            
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to GenerateLogFile");
            System.exit(INVALID_ARGUMENTS);
        }            
    }
    
    
    /**
     * Return a matching file for the given source file path and prefix.
     * 
     * @return the source file name.
     */
    private String getSourceArchiveFileName(final String validatedFileName) {
        String methodName = "getSourceArchiveFileName";
        String fileName = null;
        LOGGER.entering(CLASSNAME, methodName);
        try {
            FileInputStream fstream = new FileInputStream(sourceFilePath + File.separator + validatedFileName);
            InputStream is = new UnicodeBOMInputStream(fstream).skipBOM();
            InputStreamReader in = new InputStreamReader(is, "UTF-8");
            BufferedReader br = new BufferedReader(in);
            String strLine;                 
            // Get FirstRow and last column data for file name
            int count = 0;
            while ((strLine = br.readLine()) != null) {                
                if (!strLine.trim().equals("")) {
                    if (count == 1) {
                        if (!strLine.split(DELIMITER)[0].equals(FOOTER)) { 
                            String[] data = strLine.split(DELIMITER);
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " file content for first row data: " + strLine);
                            fileName = data[data.length - 1];
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " File name in first row data: " + fileName);
                            break;
                        }
                    }                                   
                }
                count++;
            }   
            
            br.close();
            in.close();
            is.close();
            fstream.close();
        } catch (Exception e) {
            logStackTrace(e, methodName);
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);        
        return fileName;        
    }
    
    
    /**
     * Log the Stack Trace
     * 
     * @param aThrowable - the throwable.
     * @param methodName - the method name.
     */
    private static void logStackTrace(final Throwable aThrowable, final String methodName) {
        Writer result = new StringWriter();
        aThrowable.printStackTrace(new PrintWriter(result));
        LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, result.toString());
    }
}

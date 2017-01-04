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

public final class GenerateOfferLogsFile {

    private static final String CLASSNAME = GenerateOfferLogsFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    static final String DELIMITER = "\\|";
    private static final String FOOTER = "FTRAIL";
    private static final int FIXED_COL = 26;
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
    private static final int DEST_FILE_PATH      = 8;
    
    private static final String LOG_FOLDER = "logs";

    private static final int NUM_ARGUMENTS = 9;
    
    private final String sourceFilePath;
    private final String destFilePath;
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
    public GenerateOfferLogsFile(final String sourceFilePath, final String sourceFilePrefix,
            final String validatedFileName, String destFilePath) {
        this.sourceFilePath    = sourceFilePath;
        this.sourceFilePrefix  = sourceFilePrefix;
        this.validatedFileName = validatedFileName;
        this.destFilePath      = destFilePath;
    }

    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
    public void generateLogFile(String tableName, String jdbcDriver, String dbURL, String dbUserName, String dbPassword) {
        String methodName = "generateLogFile"; 
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "tableName:" + tableName);         
         
        String sourceArchiveFileName = getSourceArchiveFileName(validatedFileName);

        try {
            String inputFilePath  = sourceFilePath + File.separator + ARCHIVE_FOLDER + File.separator + sourceArchiveFileName;
            String outputFilePath = destFilePath   + File.separator + LOG_FOLDER     + File.separator + sourceArchiveFileName.replaceFirst("attribute", "offers") + LOG_FILE_EXTENSION;
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " sourceArchiveFileName:" + sourceArchiveFileName 
                    + " inputFilePath:" + inputFilePath + " outputFilePath:" + outputFilePath);

            File           logFile = new File(outputFilePath);
            BufferedWriter logBw   = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile), "UTF-8"));
            
            List<String> logData = new ArrayList<String>();
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "table name XINT_OFFERSDATA and XINT_ATTRIBUTEDATA");

            MonitiseEtlHelper monitiseEtlHelper = new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            logData = monitiseEtlHelper.getErrorCodeWithCSVData(TableName.XINT_OFFERSATTRIBUTEDATA, sourceArchiveFileName);
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();

            // Get header
            if (logData != null) {
                
                String prevCPCODE      = "";
                String prevCPSKU       = "";
                String prevCPPARENTSKU = "";
                String prevNAME        = "";
                String prevPRODUCTTYPE = "";
                
                List<String> listAttrName      = new ArrayList<String>();
                List<String> listAttrValue     = new ArrayList<String>();
                List<String> listCommentsValue = new ArrayList<String>();
                
                for (int count1 = 0; count1 < logData.size(); count1++) {
                    if (count1 > 0) {
                        String attrName = logData.get(count1).split(DELIMITER)[FIXED_COL];
                        if (!listAttrName.contains(attrName)) {
                            listAttrName.add(attrName);
                        }
                    }
                }

                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "listAttrName:" + listAttrName);
                
                for (int count = 0; count < logData.size(); count++) {
                    
                    if (count == 0) {
                        String[] headerData = logData.get(count).split(DELIMITER);
                        prevCPCODE = headerData[0];
                        for (int col = 0; col < FIXED_COL; col++) {
                            logBw.write("|" + headerData[col].toLowerCase());
                        }

                        for (int attr = 0; attr < listAttrName.size(); attr++) {
                            logBw.write("|" + listAttrName.get(attr));
                        }
                        
                        logBw.newLine();
                    }
                    
                    if (count == 1) {
                        String[] rowData = logData.get(count).split(DELIMITER);
                        
                        prevCPCODE      = rowData[0];
                        prevCPSKU       = rowData[1];
                        prevCPPARENTSKU = rowData[2];
                        prevNAME        = rowData[3];
                        prevPRODUCTTYPE = rowData[4];
                        
                        listAttrValue.add(rowData[FIXED_COL+1]);
                        
                        if (rowData.length == (FIXED_COL+2)) {
                            listCommentsValue.add(" Loaded successfully");
                        } else {
                            listCommentsValue.add(rowData[FIXED_COL+2]);
                        }
                        
                    } else if (count > 1 && count != logData.size() - 1) {
                        
                        String[] rowData = logData.get(count).split(DELIMITER);                                              
                        
                        String curCPCODE      = rowData[0];
                        String curCPSKU       = rowData[1];
                        String curCPPARENTSKU = rowData[2];
                        String curNAME        = rowData[3];
                        String curPRODUCTTYPE = rowData[4];
                        
                        if (!(prevCPCODE.equals(curCPCODE) 
                              && prevCPSKU.equals(curCPSKU)
                              && prevCPPARENTSKU.equals(curCPPARENTSKU) 
                              && prevNAME.equals(curNAME) 
                              && prevPRODUCTTYPE.equals(curPRODUCTTYPE))) {
                            
                            String[] prevRowData = logData.get(count - 1).split(DELIMITER);
                            for (int col = 0; col < FIXED_COL; col++) {
                                logBw.write("|" + prevRowData[col]);
                            }
                            
                            for (int aval = 0; aval < listAttrValue.size(); aval++) {
                                logBw.write("|" + listAttrValue.get(aval));
                            }
                            for (int cm = 0; cm < listCommentsValue.size(); cm++) {
                                logBw.write("|" + listCommentsValue.get(cm));
                            }
                            
                            logBw.newLine();                         
                            listAttrValue.clear();
                            listCommentsValue.clear();
                            
                            prevCPCODE      = curCPCODE;
                            prevCPSKU       = curCPSKU;
                            prevCPPARENTSKU = curCPPARENTSKU;
                            prevNAME        = curNAME;
                            prevPRODUCTTYPE = curPRODUCTTYPE;
                            
                        } else {
                            
                            listAttrValue.add(rowData[FIXED_COL+1]);                          
                            
                            String comment = null;
                            if (rowData.length == (FIXED_COL+2)) {
                                comment = " Loaded successfully";
                            } else {
                                comment = logData.get(count-1).split(DELIMITER)[FIXED_COL+2];
                            }

                            if (!listCommentsValue.contains(comment)) {
                                listCommentsValue.add(comment);
                            }
                        }                        
                    }
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
     * Two arguments must be required, the source product attributes file name,
     * and the target product attributes file name.
     * 
     * @param args - input arguments
     */
    public static void main(final String[] args) {
        String methodName = "main"; 
        LOGGER.entering(CLASSNAME, methodName); 
        
        if (args.length == NUM_ARGUMENTS 
            && args[SOURCE_FILE_PATH].length() > 0 
            && args[SOURCE_FILE_PREFIX].length() > 0 
            && args[VALIDATED_FILE_NAME].length() > 0 
            && args[TABLE_NAME].length() > 0 
            && args[JDBC_DRIVER].length() > 0 
            && args[DB_URL].length() > 0 
            && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0 ) {
         
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Writing log file for attribute and offer in directory : " + args[DEST_FILE_PATH]);
    
            GenerateOfferLogsFile log = new GenerateOfferLogsFile(args[SOURCE_FILE_PATH],
                                                                  args[SOURCE_FILE_PREFIX],
                                                                  args[VALIDATED_FILE_NAME],
                                                                  args[DEST_FILE_PATH]);
            log.generateLogFile(args[TABLE_NAME],
                                args[JDBC_DRIVER], 
                                args[DB_URL], 
                                args[DB_USERNAME], 
                                args[DB_PASSWORD]);   
        } else { 
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to GenerateOffersLogFile");
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
        LOGGER.entering(CLASSNAME, methodName);
        String fileName = null;
        try {
            FileInputStream   fstream = new FileInputStream(sourceFilePath + File.separator + validatedFileName);
            InputStream       is      = new UnicodeBOMInputStream(fstream).skipBOM();
            InputStreamReader in      = new InputStreamReader(is, "UTF-8");
            BufferedReader    br      = new BufferedReader(in);
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
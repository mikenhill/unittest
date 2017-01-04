package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_ARCHIVE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.logging.Level;

import com.salmon.dataload.utils.FileUtilities;

/**
 * This class scans the sourceFilePath for a file with the given sourceFilePrefix. If found, it ensures that the number of data rows in the interface
 * file matches that in the trailer record returning INVALID_RECORD_COUNT if not. The sourceFile is then archived.
 * 
 * @author Stephen Gair
 * @revision : 1.0
 * @Date : 19 December 2011
 */
public final class DeleteJSONLoadFile {

    static final String CSV = ".csv";
    static final String TRAILER_DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    static final String COLUMN_FILENAME = "filename";
    private static final String CLASSNAME = DeleteJSONLoadFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("ddMMyyyy_hhmmss");

    private static final String ARCHIVE_FOLDER = "archive";
    private static final int NUM_ARGUMENTS = 2;
    private static final int SOURCE_FILE_PATH = 0;
    private static final int SOURCE_FILE_PREFIX = 1;
    private static final int DATE_LENGTH = 15;

    private final String sourceFilePath;
    private final String sourceFilePrefix;
    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param validatedFileName - validated file name
     * @param expectedHeader - the expected header
     */
    public DeleteJSONLoadFile(final String sourceFilePath, final String sourceFilePrefix) {
        this.sourceFilePath = sourceFilePath;
        this.sourceFilePrefix = sourceFilePrefix;
    }

    /**
     * Three arguments must be passed representing the source file path, the source file prefex and the validated file name.
     * 
     * @param args - input parameters
     */
    @SuppressWarnings("null")
    public static void main(final String[] args) { 
        DeleteJSONLoadFile validate=null;
        if (args.length == NUM_ARGUMENTS && args[SOURCE_FILE_PATH].length() > 0 && args[SOURCE_FILE_PREFIX].length() > 0               
        ) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating file: " + args[SOURCE_FILE_PATH]);
            
            validate = new DeleteJSONLoadFile(args[SOURCE_FILE_PATH], args[SOURCE_FILE_PREFIX]);
            validate.run();
            
        } else {           
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments passed to DeleteXMLLoadFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }




    /**
     * Return a Date corresponding to the date representation in the file name.
     * 
     * @param fileName - the name of the file.
     * @return a date or null, if there is no valid date.
     */
    @SuppressWarnings("null")
    private String getFileExtension(final String fileName) {   
        LOGGER.entering(CLASSNAME, "getFileExtension"); 
        int j = fileName.lastIndexOf('.');        
        String date = fileName.substring(j, fileName.length());        
        return date;
    }

    
    /**
     * Return a Date corresponding to the date representation in the file name.
     * 
     * @param fileName - the name of the file.
     * @return a date or null, if there is no valid date.
     */
    @SuppressWarnings("null")
    private Date getDatePart(final String fileName) {
        LOGGER.entering(CLASSNAME, "getDatePart"); 
        Date date = null;
        int j = fileName.lastIndexOf('.');
        if (j >= DATE_LENGTH) {
            String d = fileName.substring(j - DATE_LENGTH, j);
            try {
                date = FILE_NAME_DATE_FORMAT.parse(d);
            } catch (ParseException e) {
                date = null;
            }
        }
        LOGGER.exiting(CLASSNAME, "getDatePart"); 
        return date;
    }

    /**
     * Read the input source file. If the trailer count matches the number of records, then output output then write the validated file and archive
     * the source file.
     */
    public void run() {
        LOGGER.entering(CLASSNAME, "run");                                    
        if(getSourceFile() !=null){
            String sourceFileName = getSourceFile();
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating file: " + sourceFileName);
            File sourceFile = new File(sourceFilePath, sourceFileName);
            boolean flag = sourceFile.delete();
            if(!flag){ // if file still not deleted..
                archive(sourceFileName);
            }
        }
        LOGGER.exiting(CLASSNAME, "run"); 
    }



    /**
     * Return a matching file for the given source file path and prefix.
     * 
     * @return the source file name.
     */
    private String getSourceFile() {
        String fileName = null;
        File dir = new File(sourceFilePath);
        String[] files = dir.list();
        TreeMap<Date, String> listOfValidFiles = new TreeMap<Date, String>();
        if (files != null && files.length > 0) {
            for (String file : files) {
                // Should change to equals? - we are passing in the full file name for 
                // MOST interfaces now
                if (file.startsWith(sourceFilePrefix) && file.endsWith(".json")) {
                    Date date = this.getDatePart(file);
                    if (date != null) {
                        listOfValidFiles.put(date, file);
                    }
                }
            }
            if (!listOfValidFiles.isEmpty()) {
                // putting values in navigable set
                NavigableSet nset=listOfValidFiles.descendingKeySet();
                Date firstDate = (Date)nset.first();                
                LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "firstDate:" + firstDate);
                fileName = listOfValidFiles.get(firstDate);
                LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "fileName:" + fileName);
            }
            LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "Found Source File:" + fileName);
        }
        return fileName;
    }

 
    
   

    /**
     * Archive the source file to the archive folder. Delete the original source file.
     * 
     * @param sourceFileName - the source file name
     */
    private void archive(final String sourceFileName) {
        LOGGER.entering(CLASSNAME, "archive");
        LOGGER.logp(Level.INFO, CLASSNAME, "archive", "sourceFileName: " + sourceFileName);
        try {
            File sourceFile = new File(sourceFilePath + File.separator + sourceFileName);
            File archiveFile = new File(sourceFilePath + File.separator + ARCHIVE_FOLDER + File.separator + sourceFileName);
            FileUtilities.copyFile(sourceFile, archiveFile);
            sourceFile.delete();
        } catch (IOException e) {
            logStackTrace(e, "archive");
            System.exit(ERROR_WRITING_ARCHIVE_FILE);
        }
        LOGGER.exiting(CLASSNAME, "archive");
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
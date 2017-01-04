package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.EMPTY_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_ARCHIVE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_TARGET_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_HEADER;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_RECORD_COUNT;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;
import static com.salmon.dataload.iface.DataLoadConstants.TRAILER_IS_NOT_A_NUMBER;
import static com.salmon.dataload.iface.DataLoadConstants.NO_RECORDS_TO_PROCESS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.logging.Level;

import javax.naming.NamingException;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.utils.FileUtilities;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * This class scans the sourceFilePath for a file with the given sourceFilePrefix. If found, it ensures that the number of data rows in the interface
 * file matches that in the trailer record returning INVALID_RECORD_COUNT if not. The sourceFile is then archived.
 * 
 * @author Stephen Gair
 * @revision : 1.0
 * @Date : 19 December 2011
 */
public final class ValidateLoadFile {

    static final String CSV = ".csv";
    static final String TRAILER_DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    static final String COLUMN_FILENAME = "filename";
    private static final String LOG_FOLDER = "logs";
    private static final String HEADER = "header_";
    private static final String LOG_FILE_EXTENSION = ".log";
    private static final String CLASSNAME = ValidateLoadFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("ddMMyyyy_hhmmss");

    private static final String ARCHIVE_FOLDER = "archive";
    private static final int NON_RECORD_COUNT = 2;
    private static final int TRAILER_COL_NUM = 3;
    private static final int NUM_ARGUMENTS = 8;
    private static final int SOURCE_FILE_PATH = 0;
    private static final int SOURCE_FILE_PREFIX = 1;
    private static final int VALIDATED_FILE_NAME = 2;
    private static final int EXPECTED_HEADER = 3;
    private static final int JDBC_DRIVER = 4;
    private static final int DB_URL = 5;
    private static final int DB_USERNAME = 6;
    private static final int DB_PASSWORD = 7;
    private static final int DATE_LENGTH = 15;

    private final String sourceFilePath;
    private final String sourceFilePrefix;
    private final String validatedFileName;
    private final String expectedHeader;
    
    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param validatedFileName - validated file name
     * @param expectedHeader - the expected header
     */
    public ValidateLoadFile(final String sourceFilePath, final String sourceFilePrefix, final String validatedFileName, final String expectedHeader) {
        this.sourceFilePath = sourceFilePath;
        this.sourceFilePrefix = sourceFilePrefix;
        this.validatedFileName = validatedFileName;
        this.expectedHeader = expectedHeader;
    }

    /**
     * Three arguments must be passed representing the source file path, the source file prefex and the validated file name.
     * 
     * @param args - input parameters
     */
    @SuppressWarnings("null")
    public static void main(final String[] args) { 
        ValidateLoadFile validate=null;
        if (args.length == NUM_ARGUMENTS && args[SOURCE_FILE_PATH].length() > 0 && args[SOURCE_FILE_PREFIX].length() > 0
                && args[VALIDATED_FILE_NAME].length() > 0 && args[EXPECTED_HEADER].length() > 0
        && args[JDBC_DRIVER].length() > 0 && args[DB_URL].length() > 0 && args[DB_USERNAME].length() > 0 &&
        args[DB_PASSWORD].length() > 0 
        ) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating file: " + args[SOURCE_FILE_PATH]);
            
            validate = new ValidateLoadFile(args[SOURCE_FILE_PATH], args[SOURCE_FILE_PREFIX], args[VALIDATED_FILE_NAME],
                    args[EXPECTED_HEADER]);
            validate.run(args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD]);
            
        } else {
            validate.writeHeaderLogOutputFile("Invalid arguments passed to ValidateLoadFile");
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments passed to ValidateLoadFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }

    /**
     * Ensure that the trailer is valid. Return the trailer count if it can be parsed, otherwise return null.
     * 
     * @param trailer - the trailer
     * @return the trailer count
     */
    private Integer validateTrailer(final String trailer) {
        String[] t = trailer.split(TRAILER_DELIMITER);
        if (t.length == TRAILER_COL_NUM) {
            try {
                return Integer.parseInt(t[2]);
            } catch (NumberFormatException nfe) {
                LOGGER.logp(Level.SEVERE, CLASSNAME, "validateTrailer", "Trailer count " + t[2] + " is not a valid number");
                logStackTrace(nfe, "validateInt");
                System.exit(TRAILER_IS_NOT_A_NUMBER);
            }
        }
        return null;
    }

    /**
     * Check that the header record matches the expected header record.
     * 
     * @param header - the header
     * @return - indicates if the header is valid or not.
     */
    private boolean validateHeader(final String header) {

        return expectedHeader.equalsIgnoreCase(header.trim());
    }

    /**
     * Return a Date corresponding to the date representation in the file name.
     * 
     * @param fileName - the name of the file.
     * @return a date or null, if there is no valid date.
     */
    @SuppressWarnings("null")
    private String getFileExtension(final String fileName) {        
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
        return date;
    }

    /**
     * Read the input source file. If the trailer count matches the number of records, then output output then write the validated file and archive
     * the source file.
     */
    public void run(String jdbcDriver, String dbURL, String dbUserName, String dbPassword ) {
        String sourceFileName = getSourceFile();        
        if (sourceFileName == null) {
            writeHeaderLogOutputFile("No file with prefix " + sourceFilePrefix
                    + " and a valid date in the format ddMMYYYY_hhmmss has beeen found");
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "No file with prefix " + sourceFilePrefix
                    + " and a valid date in the format ddMMYYYY_hhmmss has beeen found");
            
            System.exit(NO_SOURCE_FILE_FOUND);
        }
        
        LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating file: " + sourceFileName);             
        boolean fileExists=false;
        try{
            MonitiseEtlHelper  monitiseEtlHelper =  new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            fileExists=monitiseEtlHelper.fileExists(sourceFileName);
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();            
        }catch(SQLException sql){
            writeHeaderLogOutputFile("SQLException Exception"+sql.getMessage());
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, "run");
            archive(sourceFileName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }catch(ClassNotFoundException cnfe){
            writeHeaderLogOutputFile("ClassNotFoundException Exception"+cnfe.getMessage());
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "ClassNotFoundException Exception"+cnfe.getMessage());
            logStackTrace(cnfe, "run");
            archive(sourceFileName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }catch(NamingException ne){
            writeHeaderLogOutputFile("NamingException Exception"+ne.getMessage());
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "NamingException Exception"+ne.getMessage());
            logStackTrace(ne, "run");
            archive(sourceFileName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }
        if (fileExists) {
            writeHeaderLogOutputFile("File " +sourceFileName + " is already processed");
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main","File " +sourceFileName + " is already processed");
            archive(sourceFileName);
            System.exit(EMPTY_FILE);
        }
        
        File sourceFile = new File(sourceFilePath, sourceFileName);
        long totalLinesCount = FileUtilities.getTotalLinesWithoutSpaceCount(sourceFile);
        if (totalLinesCount == 0) {
            writeHeaderLogOutputFile("File " +sourceFileName + " is empty");
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", sourceFileName + " is empty");
            archive(sourceFileName);
            System.exit(EMPTY_FILE);
        }
        String header = readHeader(sourceFile);
        String fileExtension=getFileExtension(sourceFileName);
        if(fileExtension.equals(".xsv")){
            header=header.trim();
            header=header.replaceAll("\\\"", "");
            header=header.replaceAll(",", "|");              
        }else   if(fileExtension.equals(".csv")){
            header=header.trim();
            header=header.replaceAll(",", "|");              
        }else if(fileExtension.equals(".tsv")){
            header=header.trim();            
            header=header.replaceAll("\t", "|");               
        }
        boolean validHeader = validateHeader(header);
        if (!validHeader) {
            writeHeaderLogOutputFile("The header " + header + " is not valid, expecting: " + expectedHeader);
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "The header " + header + " is not valid, expecting: " + expectedHeader);
            archive(sourceFileName);
            System.exit(INVALID_HEADER);
        }else {
            writeOutputFile(sourceFile, totalLinesCount, fileExtension);
            archive(sourceFileName);
            return;
        }       
    }

    /**
     * Read and return the source file header
     * 
     * @param sourceFile - the file to be read.
     * @return the header
     */
    public String readHeader(final File sourceFile) {
        LOGGER.entering(CLASSNAME, "readHeader");
        String strLine = "";
        try {
            InputStream fis = getFileInputStream(sourceFile);
            InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader br = new BufferedReader(in);
            strLine = br.readLine();
            in.close();
        } catch (Exception e) {
            logStackTrace(e, "readHeader");
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, "readHeader");
        return strLine;
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
                if (file.startsWith(sourceFilePrefix) && !file.endsWith(".xml")) {
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
     * Write the Log Header output file (which is the same as the input file name_header).      
     * @param sourceFile - the source file
     * @param lineCount - the number of lines in the source file
     */
    private void writeHeaderLogOutputFile(String strLine) {
        LOGGER.entering(CLASSNAME, "writeHeaderLogOutputFile", strLine);
        try {
            String sourceFileName = getSourceFile();
            File sourceFile = new File(sourceFilePath+ File.separator +  LOG_FOLDER + File.separator +HEADER+sourceFileName+LOG_FILE_EXTENSION);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sourceFile), "UTF-8"));
                    bw.write(strLine);
                    bw.newLine();               
                    bw.close();
        } catch (Exception e) {
            logStackTrace(e, "writeHeaderLogOutputFile");
            System.exit(ERROR_WRITING_TARGET_FILE);
        }
        LOGGER.exiting(CLASSNAME, "writeHeaderLogOutputFile");

    }
    
    private InputStream getFileInputStream(File sourceFile) throws FileNotFoundException {
        return new FileInputStream(sourceFile);
    }
   
    /**
     * Write the validated output file (which is the same as the input file without the trailer). Also, append the source file name as the final
     * column.
     * 
     * @param sourceFile - the source file
     * @param lineCount - the number of lines in the source file
     */
    private void writeOutputFile(final File sourceFile, final long lineCount, final String  fileExtension) {
        LOGGER.entering(CLASSNAME, "writeOutputFile", validatedFileName);
        try {
            File file = new File(validatedFileName);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));
            InputStream fis = getFileInputStream(sourceFile);
            InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader br = new BufferedReader(in);

            for (int i = 0; i < lineCount; ) {
                String strLine = br.readLine();
                if(fileExtension.equals(".xsv")){
                    strLine=strLine.trim();
                    strLine=strLine.replaceAll("\\\"", "");
                    strLine=strLine.replaceAll(",", "|");              
                }else if(fileExtension.equals(".csv")){
                    strLine=strLine.trim();                    
                    strLine=strLine.replaceAll(",", "|");              
                }else if(fileExtension.equals(".tsv")){
                    strLine=strLine.trim();            
                    strLine=strLine.replaceAll("\t", "|");               
                }
                if(!strLine.equals("")){
                    if (i == 0) {
                        bw.write(strLine + COLUMN_DELIMITER + COLUMN_FILENAME);
                    } else {
                        if(fileExtension.equals(".xsv")){
                            bw.write(strLine + COLUMN_DELIMITER + sourceFile.getName().replaceFirst("xsv", "xml"));
                        }else{
                            bw.write(strLine + COLUMN_DELIMITER + sourceFile.getName());   
                        }
                    }
                
                    i++;
                }
                bw.newLine();
            }
            in.close();
            bw.close();
        } catch (Exception e) {
            logStackTrace(e, "writeOutputFile");
            System.exit(ERROR_WRITING_TARGET_FILE);
        }
        LOGGER.exiting(CLASSNAME, "writeOutputFile");

    }

    /**
     * Archive the source file to the archive folder. Delete the original source file.
     * 
     * @param sourceFileName - the source file name
     */
    private void archive(final String sourceFileName) {
        LOGGER.entering(CLASSNAME, "archive");
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
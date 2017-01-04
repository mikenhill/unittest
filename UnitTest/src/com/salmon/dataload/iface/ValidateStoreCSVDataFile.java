package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.EMPTY_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_HEADER;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.logging.Level;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.helper.TableName;
import com.salmon.dataload.utils.FileUtilities;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * This class scans the sourceFilePath for a file with the given sourceFileName. 
 * 
 * @author Pranava Mishra
 * @revision : 1.0
 * @Date : 25 May 2014
 */
public final class ValidateStoreCSVDataFile {

    static final String CSV = ".csv";
    static final String TRAILER_DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    static final String DELIMITER = "\\|";
    static final String COLUMN_FILENAME = "filename";

    private static final String CLASSNAME = ValidateStoreCSVDataFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);    

    private static final int NUM_ARGUMENTS       = 8;
    private static final int SOURCE_FILE_PATH    = 0;
    private static final int SOURCE_FILE_NAME    = 1;
    private static final int VALIDATED_FILE_NAME = 2;
    private static final int EXPECTED_HEADER     = 3;
    private static final int JDBC_DRIVER         = 4;
    private static final int DB_URL              = 5;
    private static final int DB_USERNAME         = 6;
    private static final int DB_PASSWORD         = 7;

    private static final int CPCODE     = 0; 
    private static final int STORE_CODE = 1; 
    private static final int STORE_NAME = 2;  
    private static final int LATITUDE = 13;  
    private static final int LONGITUDE = 14;  
    
    private final String sourceFilePath;
    private final String sourceFileName;
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
    public ValidateStoreCSVDataFile(final String sourceFilePath, final String sourceFileName, final String validatedFileName, final String expectedHeader) {
        this.sourceFilePath    = sourceFilePath;
        this.sourceFileName    = sourceFileName;
        this.validatedFileName = validatedFileName;
        this.expectedHeader    = expectedHeader;
    }

    /**
     * Three arguments must be passed representing the source file path, the source file prefex and the validated file name.
     * 
     * @param args - input parameters
     */
    public static void main(final String[] args) {
        String methodName = "main";
        if (args.length == NUM_ARGUMENTS 
            && args[SOURCE_FILE_PATH].length() > 0 
            && args[SOURCE_FILE_NAME].length() > 0                
            && args[VALIDATED_FILE_NAME].length() > 0 
            && args[EXPECTED_HEADER].length() > 0        
            && args[JDBC_DRIVER].length() > 0 
            && args[DB_URL].length() > 0 
            && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0 ) 
        {
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating file: " + args[SOURCE_FILE_PATH]);            
            ValidateStoreCSVDataFile validate = new ValidateStoreCSVDataFile(args[SOURCE_FILE_PATH], 
                                                                             args[SOURCE_FILE_NAME], 
                                                                             args[VALIDATED_FILE_NAME], 
                                                                             args[EXPECTED_HEADER]);
            validate.run(args[JDBC_DRIVER], 
                         args[DB_URL], 
                         args[DB_USERNAME], 
                         args[DB_PASSWORD]);            
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to ValidateStockCSVDataFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }


    /**
     * Read the input source file. If the trailer count matches the number of records, then output output then write the validated file and archive
     * the source file.
     */
    private void run(String jdbcDriver, String dbURL, String dbUserName, String dbPassword ) {   
        String methodName = "run";
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating data of CSV file: ");                                     
        File sourceFile = new File(sourceFilePath, validatedFileName);
        File sourceFileNew = new File(sourceFilePath, validatedFileName);
        long totalLinesCount = FileUtilities.getTotalLinesCount(sourceFile);
        if (totalLinesCount == 0) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, sourceFileName + " is empty");            
            System.exit(EMPTY_FILE);
        }
        String header = readHeader(sourceFile);
        boolean validHeader = validateHeader(header);        
        if (!validHeader) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "The header " + header + " is not valid, expecting: " + expectedHeader);
            System.exit(INVALID_HEADER);
        }
        
        try{
            MonitiseEtlHelper  monitiseEtlHelper =  new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            validateCSVData(monitiseEtlHelper, sourceFileNew, totalLinesCount);
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();            
        }catch(SQLException sql){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }catch(ClassNotFoundException cnfe){
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "ClassNotFoundException Exception"+cnfe.getMessage());
            logStackTrace(cnfe, methodName);
            System.exit(NO_SOURCE_FILE_FOUND);        
        }      
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
     * Read and return the source file header
     * 
     * @param sourceFile - the file to be read.
     * @return the header
     */
    public static String readHeader(final File sourceFile) {
        String methodName = "readHeader";
        LOGGER.entering(CLASSNAME, methodName);
        String strLine = "";
        try {
            FileInputStream fis = new FileInputStream(sourceFile);
            InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader br = new BufferedReader(in);
            strLine = br.readLine();
            in.close();
        } catch (Exception e) {
            logStackTrace(e, methodName);
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);
        return strLine;
    }


    /**
     * Write the validated output file data (which is the same as the input file without the trailer and with append the source file name).      
     * @param sourceFile - the source file
     * @param lineCount - the number of lines in the source file
     */
    public void validateCSVData(MonitiseEtlHelper  monitiseEtlHelper,File sourceFile, final long lineCount) throws SQLException {
        String methodName = "validateCSVData";
        LOGGER.entering(CLASSNAME, methodName, validatedFileName);
        
        try {            
            InputStream   fis         = getFileInputStream(sourceFile);
            InputStream       cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in          = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader    br          = new BufferedReader(in);
            
            int headerLength=0;
            
            for (int i = 0; i < lineCount; i++) {
                String strLine = br.readLine(); 
                
                if (i == 0) {
                    headerLength=strLine.split(DELIMITER).length;
                }
                
                if (i > 0) {
                    
                    String[] dataArray=strLine.split(DELIMITER);
                    String eCode="";
                    int index=0;
                    int rowDataLength=dataArray.length; 
                    
                    if (headerLength != rowDataLength) {
                        String errorrowDataLength="E016";
                        eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorrowDataLength!="")?COLUMN_DELIMITER+errorrowDataLength:errorrowDataLength):errorrowDataLength;
                    }
                    
                    for(String data : dataArray){
                        switch(index){
                        case CPCODE: 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, data + " is cpcode");
                            String errorCPCode=monitiseEtlHelper.isValidCpCode(data)!=null?monitiseEtlHelper.isValidCpCode(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorCPCode!="")?COLUMN_DELIMITER+errorCPCode:errorCPCode):errorCPCode;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+eCode + " for cpcode");
                            break;  
                        case STORE_CODE: 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, data + " is store code");
                            String errorStoreCode=monitiseEtlHelper.isValidStoreCode(data)!=null?monitiseEtlHelper.isValidStoreCode(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorStoreCode!="")?COLUMN_DELIMITER+errorStoreCode:errorStoreCode):errorStoreCode;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+eCode + " for store code");
                            break;     
                        case STORE_NAME: 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, data + " is store name");
                            String errorStoreName=monitiseEtlHelper.isValidStoreName(data)!=null?monitiseEtlHelper.isValidStoreName(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorStoreName!="")?COLUMN_DELIMITER+errorStoreName:errorStoreName):errorStoreName;
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : "+eCode + " for store name");
                            break;     
                        case LATITUDE: 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, data + " is latitude");
                            String errorLatitude=monitiseEtlHelper.isValidNumericValue(data, "Latitude")!=null?monitiseEtlHelper.isValidNumericValue(data, "Latitude"):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorLatitude!="")?COLUMN_DELIMITER+errorLatitude:errorLatitude):errorLatitude; 
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "comments code : " + errorLatitude + " for latitude");
                            break;
                         
                        
                        }
                        
                        index++;
                    }
                    String status=(eCode!=null && eCode.length() > 0)?"2":"1";
                    monitiseEtlHelper.updateErrorCode(TableName.XINT_STORESDATA, dataArray, eCode, status);                        
                }                
            }
            br.close();
            in.close();
        } catch (SQLException sqle) {
            logStackTrace(sqle, methodName);                      
        } catch (Exception e) {
            logStackTrace(e, methodName);                        
        } finally {
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();
        }
        LOGGER.exiting(CLASSNAME, methodName);
    }
    
    private InputStream getFileInputStream(File sourceFile) throws FileNotFoundException {
        return new FileInputStream(sourceFile);
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
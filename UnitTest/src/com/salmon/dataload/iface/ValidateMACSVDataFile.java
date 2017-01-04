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
public final class ValidateMACSVDataFile {

    static final String CSV = ".csv";
    static final String TRAILER_DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    static final String DELIMITER = "\\|";
    static final String COLUMN_FILENAME = "filename";

    private static final String CLASSNAME = ValidateMACSVDataFile.class.getName();
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
    
    private static final int CPCODE      = 0;
    private static final int CPSOURCESKU = 1;
    private static final int MASTYPE     = 2;
    private static final int CPTARGETSKU = 3;  
    
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
    public ValidateMACSVDataFile(final String sourceFilePath, final String sourceFileName, final String validatedFileName, final String expectedHeader) {
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
        if (args.length == NUM_ARGUMENTS && args[SOURCE_FILE_PATH].length() > 0 && args[SOURCE_FILE_NAME].length() > 0
            && args[VALIDATED_FILE_NAME].length() > 0 && args[EXPECTED_HEADER].length() > 0
            && args[JDBC_DRIVER].length() > 0 && args[DB_URL].length() > 0 && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0 
        ) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating file: " + args[SOURCE_FILE_PATH]);            
            ValidateMACSVDataFile validate = new ValidateMACSVDataFile(args[SOURCE_FILE_PATH], args[SOURCE_FILE_NAME], args[VALIDATED_FILE_NAME],
                    args[EXPECTED_HEADER]);
            validate.run(args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD]);
            
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments passed to ValidateStockCSVDataFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }


    /**
     * Read the input source file. If the trailer count matches the number of records, then output output then write the validated file and archive
     * the source file.
     */
    private void run(String jdbcDriver, String dbURL, String dbUserName, String dbPassword ) {      
        LOGGER.logp(Level.INFO, CLASSNAME, "main", "Validating data of CSV file: ");                                     
        File sourceFile = new File(sourceFilePath, validatedFileName);   
        long totalLinesCount = FileUtilities.getTotalLinesCount(sourceFile);
        if (totalLinesCount == 0) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", sourceFileName + " is empty");            
            System.exit(EMPTY_FILE);
        }
        String header = readHeader(sourceFile);
        boolean validHeader = validateHeader(header);        
        if (!validHeader) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "The header " + header + " is not valid, expecting: " + expectedHeader);
            System.exit(INVALID_HEADER);
        }
        try{
            MonitiseEtlHelper  monitiseEtlHelper =  new MonitiseEtlHelper(jdbcDriver, dbURL, dbUserName, dbPassword);
            validateCSVData(monitiseEtlHelper, sourceFile, totalLinesCount);
            monitiseEtlHelper.commit();
            monitiseEtlHelper.close();            
        }catch(SQLException sql){
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "SQLException Exception"+sql.getMessage());
            logStackTrace(sql, "run");
            System.exit(NO_SOURCE_FILE_FOUND);        
        }catch(ClassNotFoundException cnfe){
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "ClassNotFoundException Exception"+cnfe.getMessage());
            logStackTrace(cnfe, "run");
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
        LOGGER.entering(CLASSNAME, "readHeader");
        String strLine = "";
        try {
            FileInputStream fis = new FileInputStream(sourceFile);
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
     * Write the validated output file data (which is the same as the input file without the trailer and with append the source file name).      
     * @param sourceFile - the source file
     * @param lineCount - the number of lines in the source file
     */
    public void validateCSVData(MonitiseEtlHelper  monitiseEtlHelper,File sourceFile, final long lineCount) throws SQLException {
        LOGGER.entering(CLASSNAME, "validateCSVData", validatedFileName);
        try {
            
            InputStream fis = getFileInputStream(sourceFile);
            InputStream cleanStream = new UnicodeBOMInputStream(fis).skipBOM();
            InputStreamReader in = new InputStreamReader(cleanStream, "UTF-8");
            BufferedReader br = new BufferedReader(in);
            int headerLength=0;
            for (int i = 0; i < lineCount; i++) {
                String strLine = br.readLine();   
                if(i==0){
                    headerLength=strLine.split(DELIMITER).length;
                }
                if(i>0)
                {
                    String[] dataArray=strLine.split(DELIMITER);
                    String eCode="";
                    int index=0;
                    
                    if(headerLength!=dataArray.length){
                        String errorrowDataLength="E016";
                        eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorrowDataLength!="")?COLUMN_DELIMITER+errorrowDataLength:errorrowDataLength):errorrowDataLength;
                    }
                    
                    //CP Code|CP Source sku|CP Target sku|maAssociation
                    for(String data : dataArray){
                        switch(index){
                        case CPCODE: 
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData", dataArray[index] + " is cpcode");                            
                            String errorCPCode=monitiseEtlHelper.isValidCpCode(data)!=null?monitiseEtlHelper.isValidCpCode(data):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorCPCode!="")?COLUMN_DELIMITER+errorCPCode:errorCPCode):errorCPCode;                            
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData", "comments code : "+eCode + " for cpcode");
                            break;
                        case CPSOURCESKU:
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData",  dataArray[index] + " is cpsourcesku");
                            String errorSOURCESKU=monitiseEtlHelper.isValidSku(data, "sourceSku")!=null?monitiseEtlHelper.isValidSku(data, "sourceSku"):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorSOURCESKU!="")?COLUMN_DELIMITER+errorSOURCESKU:errorSOURCESKU):errorSOURCESKU; 
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData", "comments code : "+errorSOURCESKU + " for cpsku");                         
                            break;                            
                        case MASTYPE:
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData",  dataArray[index] + " is ma type");
                            String errorMasType=monitiseEtlHelper.isValidPreDefinedValue(data, "merchandisingAssociations");
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorMasType!="")?COLUMN_DELIMITER+errorMasType:errorMasType):errorMasType; 
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData", "comments code : "+errorMasType + " for ma type");                                                                                  
                            break;                              
                        case CPTARGETSKU:
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData",  dataArray[index] + " is cptargetsku");
                            String errorTargetSKU=monitiseEtlHelper.isValidSku(data, "targetSku")!=null?monitiseEtlHelper.isValidSku(data, "targetSku"):"";
                            eCode=(eCode!=null && eCode.length() > 0)? eCode+((errorTargetSKU!="")?COLUMN_DELIMITER+errorTargetSKU:errorTargetSKU):errorTargetSKU; 
                            LOGGER.logp(Level.INFO, CLASSNAME, "validateCSVData", "comments code : "+errorTargetSKU + " for cptargetsku");
                            break;
                        }
                      index++;
                    }
                        String status=(eCode!=null && eCode.length() > 0)?"2":"1";
                        monitiseEtlHelper.updateErrorCode(TableName.XINT_MASDATA, dataArray, eCode, status);                  
                }                
            }
            br.close();
            in.close();
        } catch (SQLException sqle) {
            logStackTrace(sqle, "validateCSVData");                      
        } catch (Exception e) {
            logStackTrace(e, "validateCSVData");                        
        }
        LOGGER.exiting(CLASSNAME, "validateCSVData");

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
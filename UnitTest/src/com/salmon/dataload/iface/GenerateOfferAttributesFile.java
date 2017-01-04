package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * Generates the Product Attributes interface file from the Product attributes file. It takes the Identifier and Value Identifier columns from the
 * Products file and creates data rows in the Product Attributes file, copying these columns into the Identifier, and Allowed value columns
 * respectively.
 * 
 * @author Stephen Gair
 * @revision : 1.0
 * @Date : 19 December 2011
 */

public final class GenerateOfferAttributesFile {

    private static final String CLASSNAME = GenerateOfferAttributesFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    static final String DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    private static final String FOOTER = "FTRAIL";
    private static final int SOURCE_FILE_PATH = 0;
    private static final int SOURCE_FILE_PREFIX = 1;
    private static final int DEST_PRODUCT_FILE_PATH = 2;
    private static final int DEST_PRODUCT_FILE_PREFIX = 3;
    private static final int DEST_ATTRIBUTE_FILE_PATH = 4;
    private static final int DEST_ATTRIBUTE_FILE_PREFIX = 5;
    private static final int DEST_PRODUCT_FILE_HEADER = 6;
    private static final int DEST_ATTR_FILE_HEADER = 7;
    private static final int JDBC_DRIVER = 8;
    private static final int DB_URL = 9;
    private static final int DB_USERNAME = 10;
    private static final int DB_PASSWORD = 11;
    private static final String validatedFileName="validated.offers.import.csv";
    
    private static final int DATE_LENGTH = 15;
    private static final int NUM_ARGUMENTS = 12;  
    private static int SEQUENCE=0;
    
    private static final int CPCODE         = 0;
    private static final int PRODUCTTYPE    = 1;
    private static final int CPSKU          = 2;
    
    private final String sourceFilePath;
    private final String sourceFilePrefix;
    private final String destAttributeFilePath;;
    private final String destAttributeFilePrefix;
    private final String destProductFilePath;;
    private final String destProductFilePrefix;
    private final String destProductFileHeader;
    private final String destAttrFileHeader;

    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param validatedFileName - validated file name
     * @param expectedHeader - the expected header
     */
    public GenerateOfferAttributesFile(final String sourceFilePath, final String sourceFilePrefix, final String destProductFilePath, final String destProductFilePrefix,final String destAttributeFilePath, final String destAttributeFilePrefix,final String destProductFileHeader ,final String destAttrFileHeader ) {
        this.sourceFilePath          = sourceFilePath;
        this.sourceFilePrefix        = sourceFilePrefix;
        this.destProductFilePath     = destProductFilePath;
        this.destProductFilePrefix   = destProductFilePrefix;
        this.destAttributeFilePath   = destAttributeFilePath;
        this.destAttributeFilePrefix = destAttributeFilePrefix;
        this.destProductFileHeader   = destProductFileHeader;
        this.destAttrFileHeader      = destAttrFileHeader;
    }
  
  
    
    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
     public void processProductFile() {
        String methodName = "processProductFile";         
        String sourceFileName = getSourceArchiveFileName(validatedFileName);
        LOGGER.entering(CLASSNAME, methodName, sourceFileName);
        int lineProductCount = 0; 

        try {
        
            String outputFilePath = destProductFilePath + File.separator + destProductFilePrefix + getDatePartName(sourceFileName);
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "output file path: " + outputFilePath);
            
            File           inputFile = new File(sourceFilePath + File.separator + validatedFileName);
            InputStream    fstream   = getFileInputStream(inputFile);
            InputStream    is        = new UnicodeBOMInputStream(fstream).skipBOM();
            BufferedReader br        = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String strLine;     
            
            File productfile = new File(outputFilePath);
            BufferedWriter productBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(productfile), "UTF-8"));
            
            // Get header
            int count = 0;
            int sequence=SEQUENCE;
            int headerLength=destProductFileHeader.split(DELIMITER).length-1;
            
            while ((strLine = br.readLine()) != null) {
                if (!strLine.trim().equals("")) {                   
                    
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Row:" + strLine);

                    strLine=strLine+"|:::::::::";  // this addition handle multile null attribute
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, " data row length " + strLine.split(DELIMITER).length);
                    if (count > 0) {                        
                        if (!strLine.split(DELIMITER)[0].equals(FOOTER)) {                            
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, strLine);
                            lineProductCount++;
                             int variantCol=1;
                             productBw.write(strLine.split(DELIMITER)[0]);
                                while(variantCol < headerLength){                                                                                                                                                                                                      
                                    productBw.write( "|" + strLine.split(DELIMITER)[variantCol]);
                                    variantCol++;
                                } 
                                productBw.write( "|" + sequence);
                                productBw.newLine();
                                sequence++;
                        } 
                        
                    } else {
                        productBw.write(destProductFileHeader);
                        productBw.newLine();
                        count++;
                    }
                } 
            }           
            is.close();
            productBw.close();
        } catch (Exception e) {
            logStackTrace(e, "product extracting");
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);
    }
    
    
    
    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
     public void processAttributeFile(MonitiseEtlHelper monitiseEtlHelper) {
         String methodName = "processAttributeFile";
         
         String sourceFileName = getSourceArchiveFileName(validatedFileName);
         LOGGER.entering(CLASSNAME, methodName, sourceFileName);
          
         int lineCount = 0; 
         try {
            String outputFilePath=destAttributeFilePath+ File.separator + destAttributeFilePrefix +getDatePartName(sourceFileName);
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, " output file path: "+outputFilePath);
                     
            File           inputFile = new File(sourceFilePath + File.separator + validatedFileName);
            InputStream    fstream   = getFileInputStream(inputFile);
            InputStream    is        = new UnicodeBOMInputStream(fstream).skipBOM();
            BufferedReader br        = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            
            String strLine;
            String[] variantHeader=null;
            String[] dataArray=null;
            
            File attrfile = new File(outputFilePath);
            BufferedWriter attrBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(attrfile), "UTF-8"));
            
            // Get header
            int count = 0;
            int sequence=SEQUENCE;
            int headerLength=destProductFileHeader.split(DELIMITER).length-1;
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "destProductFileHeader:" + destProductFileHeader);
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "headerLength:" + headerLength);
            
            int hdrCols  = 0;
            
            HashMap<String, String> mapPT       = new HashMap<String, String>();
            
            while ((strLine = br.readLine()) != null) {
                
                if (!strLine.trim().equals("")) {
                    
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Count:" + count + " Row:" + strLine);
                    
                    if(count == 0){
                        variantHeader=strLine.split(DELIMITER);
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "variantHeader:" + strLine);
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "variantHeader length:" + variantHeader.length);
                    }
                    
                    if (count > 0) {                        

                        HashMap<String, String> mapPTFields = new HashMap<String, String>();
                        
                        dataArray=strLine.split(DELIMITER);  

                        if (dataArray.length > 2) {
                            if (!mapPT.containsKey(dataArray[PRODUCTTYPE])) {
                                String allowableFields = monitiseEtlHelper.getAllowedAttributes(dataArray[PRODUCTTYPE]);
                                if (allowableFields != null) {
                                    mapPT.put(dataArray[PRODUCTTYPE], allowableFields);
                                }                                
                            }
                            
                            if (mapPT.containsKey(dataArray[PRODUCTTYPE])) {
                                String allowedAttributes = mapPT.get(dataArray[PRODUCTTYPE]);
                                String[] attributeArray=allowedAttributes.split(DELIMITER);
                                for(int j=0, max = attributeArray.length; j < max; j++) {                                    
                                    String[] attributeProperty = attributeArray[j].split(",");
                                    mapPTFields.put(attributeProperty[0], dataArray[PRODUCTTYPE]);
                                }       
                            }
                        }
                       
                        if (!dataArray[0].equals(FOOTER)) {  
                            
                            if(variantHeader.length > headerLength) {
                                
                                int variantCol=0; 
                                int noDataCols=dataArray.length - headerLength;
                                
                                while((headerLength + variantCol) < variantHeader.length-1) {
                                    
                                    if (mapPTFields.containsKey(variantHeader[(headerLength + variantCol)])) {
                                    
                                        attrBw.write(dataArray[CPCODE] + "|" + dataArray[PRODUCTTYPE] + "|" + dataArray[CPSKU]); 
                                        
                                        attrBw.write( "|" + variantHeader[(headerLength + variantCol)]);
                                        
                                        if (dataArray[(headerLength+variantCol)].startsWith(destProductFilePrefix)) {
                                            attrBw.write( "|" );
                                        } else {
                                            attrBw.write( "|" + dataArray[(headerLength+variantCol)]);
                                        }
                                        
                                        attrBw.write( "|" + sequence);
                                        attrBw.newLine();
                                        lineCount++;  
                                    }    
                                    
                                    variantCol++;    
                                    
                                    if (variantCol >= noDataCols) {
                                        variantCol = variantHeader.length-1;
                                    } 
                                }
                            }                                                         
                        } 
                      sequence++;  
                    } else {                          
                        
                        hdrCols = strLine.split(DELIMITER).length;                        
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "hdrCols:" + hdrCols);
                        
                        attrBw.write(destAttrFileHeader);
                        attrBw.newLine();
                        count++;
                    }
                } 
            }        
            is.close();
            attrBw.close();
        } catch (Exception e) {
            logStackTrace(e, "attribute extracting");
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
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "arguments passed to GenerateOfferAttributesFile:"
                + args[SOURCE_FILE_PATH]+"::"
                + args[SOURCE_FILE_PREFIX]+"::"
                + args[DEST_PRODUCT_FILE_PREFIX]+"::"
                + args[DEST_ATTRIBUTE_FILE_PATH]+"::"
                + args[DEST_ATTRIBUTE_FILE_PREFIX]+"");
        
        if (args.length == NUM_ARGUMENTS 
            && args[SOURCE_FILE_PATH].length() > 0 
            && args[SOURCE_FILE_PREFIX].length() > 0
            && args[DEST_PRODUCT_FILE_PATH].length() > 0 
            && args[DEST_PRODUCT_FILE_PREFIX].length() > 0
            && args[DEST_ATTRIBUTE_FILE_PATH].length() > 0 
            && args[DEST_ATTRIBUTE_FILE_PREFIX].length() > 0 
            && args[DEST_PRODUCT_FILE_HEADER].length()>0  
            && args[DEST_ATTR_FILE_HEADER].length()>0
            && args[JDBC_DRIVER].length() > 0 
            && args[DB_URL].length() > 0 
            && args[DB_USERNAME].length() > 0 
            && args[DB_PASSWORD].length() > 0) {
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating file: " + args[SOURCE_FILE_PATH]);
            GenerateOfferAttributesFile generate = new GenerateOfferAttributesFile(args[SOURCE_FILE_PATH], 
                                                                                   args[SOURCE_FILE_PREFIX], 
                                                                                   args[DEST_PRODUCT_FILE_PATH],
                                                                                   args[DEST_PRODUCT_FILE_PREFIX],
                                                                                   args[DEST_ATTRIBUTE_FILE_PATH], 
                                                                                   args[DEST_ATTRIBUTE_FILE_PREFIX], 
                                                                                   args[DEST_PRODUCT_FILE_HEADER],
                                                                                   args[DEST_ATTR_FILE_HEADER]);
            try{
                MonitiseEtlHelper monitiseEtlHelper = new MonitiseEtlHelper(args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD]);
                SEQUENCE=monitiseEtlHelper.getSequence();
                generate.processProductFile();
                generate.processAttributeFile(monitiseEtlHelper);
                monitiseEtlHelper.commit();
                monitiseEtlHelper.close();            
            } catch(SQLException sql){
                LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "SQLException Exception"+sql.getMessage());
                logStackTrace(sql, methodName);
                System.exit(NO_SOURCE_FILE_FOUND);        
            } catch(ClassNotFoundException cnfe){
                LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "ClassNotFoundException Exception"+cnfe.getMessage());
                logStackTrace(cnfe, methodName);
                System.exit(NO_SOURCE_FILE_FOUND);        
            }  
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Invalid arguments passed to ExtractOfferFile");
            System.exit(INVALID_ARGUMENTS);
        }            
    }
    

    /**
     * Return a matching file for the given source file path and prefix.
     * 
     * @return the source file name.
     */
    private String getSourceArchiveFileName(String validatedFileName) {
        String fileName = null;
        LOGGER.entering(CLASSNAME, "getSourceArchiveFileName");
        try {
            FileInputStream fstream = new FileInputStream(sourceFilePath+ File.separator+validatedFileName);
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
                            String[] data=strLine.split(DELIMITER);
                            LOGGER.logp(Level.INFO, CLASSNAME, "getSourceArchiveFileName", " file content for first row data: " + strLine);
                            fileName=data[data.length - 1];
                            if(fileName.endsWith(".xml")){
                                fileName=fileName.replaceFirst(".xml", ".xsv"); 
                            }
                            LOGGER.logp(Level.INFO, CLASSNAME, "getSourceArchiveFileName", " File name in first row data: " + fileName);
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
            logStackTrace(e, "getSourceArchiveFileName");
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, "getSourceArchiveFileName");        
        return fileName;        
    }
    /**
     * Return a Date corresponding to the date representation in the file name.
     * 
     * @param fileName - the name of the file.
     * @return a date or null, if there is no valid date.
     */
    @SuppressWarnings("null")
    private String getDatePartName(final String fileName) {
        LOGGER.logp(Level.INFO, CLASSNAME, "getDatePartName", "Found Source File:" + fileName);
        String d="";
        int j = fileName.lastIndexOf('.');
        if (j >= DATE_LENGTH) {
             d = fileName.substring(j - DATE_LENGTH, j+4);
            
        }
        return d;
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

package com.salmon.dataload.iface; 

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_ARCHIVE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_TARGET_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_HEADER;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.utils.FileUtilities;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * Generates the Product Attributes interface file from the JSON offers file. It takes the Identifier and Value Identifier columns from the
 * Products file and creates data rows in the Product Attributes file, copying these columns into the Identifier, and Allowed value columns
 * respectively.
 * 
 * @author Pranava Mishra
 * @revision : 1.0
 * @Date : 08 Aug 2014
 */

public final class GenerateJSONOfferAttributesFile {

    private static final String CLASSNAME = GenerateJSONOfferAttributesFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    
    static final String DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    
    private static final String ARCHIVE_FOLDER = "archive";
    private static final String FOOTER = "FTRAIL";
    private static final String LOG_FOLDER = "logs";
    private static final String HEADER = "jsonheader_";
    private static final String LOG_FILE_EXTENSION = ".log";
    private static final String FILE_EXTENSION = ".psv";
    
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("ddMMyyyy_hhmmss");
    
    private static final int SOURCE_FILE_PATH           = 0;
    private static final int SOURCE_FILE_PREFIX         = 1;
    private static final int DEST_PRODUCT_FILE_PATH     = 2;
    private static final int DEST_PRODUCT_FILE_PREFIX   = 3;
    private static final int DEST_ATTRIBUTE_FILE_PATH   = 4;
    private static final int DEST_ATTRIBUTE_FILE_PREFIX = 5;
    private static final int DEST_PRODUCT_FILE_HEADER   = 6;
    private static final int DEST_ATTR_FILE_HEADER      = 7;
    private static final int JDBC_DRIVER                = 8;
    private static final int DB_URL                     = 9;
    private static final int DB_USERNAME                = 10;
    private static final int DB_PASSWORD                = 11;
    private static final int JSON_PRODUCT_FILE_HEADER   = 12;
    
    private static final int DATE_LENGTH   = 15;
    private static final int NUM_ARGUMENTS = 13;  
    
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
    private final String jsonProductHeader;
    
    private ArrayList<String> dataList     = new ArrayList<String>();
    private ArrayList<String> attrHdrList  = new ArrayList<String>();
    private ArrayList<String> attrDataList = new ArrayList<String>();
    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param validatedFileName - validated file name
     * @param expectedHeader - the expected header
     */
    public GenerateJSONOfferAttributesFile(final String sourceFilePath, final String sourceFilePrefix, final String destProductFilePath, final String destProductFilePrefix,final String destAttributeFilePath, final String destAttributeFilePrefix,final String destProductFileHeader ,final String destAttrFileHeader,final String jsonProductHeader ) {
        this.sourceFilePath          = sourceFilePath;
        this.sourceFilePrefix        = sourceFilePrefix;
        this.destProductFilePath     = destProductFilePath;
        this.destProductFilePrefix   = destProductFilePrefix;
        this.destAttributeFilePath   = destAttributeFilePath;
        this.destAttributeFilePrefix = destAttributeFilePrefix;
        this.destProductFileHeader   = destProductFileHeader;
        this.destAttrFileHeader      = destAttrFileHeader;
        this.jsonProductHeader       = jsonProductHeader; 
    }
  
  
    
    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
     public void extractJsonProductFile() {
        String methodName = "extractJsonProductFile";         
        String sourceFileName = getSourceFile();
        LOGGER.entering(CLASSNAME, methodName, sourceFileName);
        int lineProductCount = 0; 
        
        if (sourceFileName != null) {

            try {
            
                String inputFilePath=sourceFilePath+File.separator+sourceFileName;
                String outputFilePath=destProductFilePath+ File.separator + destProductFilePrefix + getDatePartName(sourceFileName)+FILE_EXTENSION;
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "input file path: " + inputFilePath + " output file path: " + outputFilePath);
                
                File            inputFile = new File(inputFilePath);
                InputStream     fstream   = getFileInputStream(inputFile);
                InputStream     is        = new UnicodeBOMInputStream(fstream).skipBOM();
                BufferedReader  br        = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                String strLine;     
                
                File productfile = new File(outputFilePath);
                BufferedWriter productBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(productfile), "UTF-8"));
                
                // Get JSON Line pipe separated
                int countL = 0;
                String line;
                StringBuffer jsonLine = new StringBuffer();
                int sline = 0;
    
                while ((line = br.readLine()) != null) {
                    
                    line = line.trim();
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "line:" + line);
                    
                    if (!line.equals("")) {
                        
                        for (int i = 0; i < line.length(); i++) {
                            char sc = line.charAt(0);
                            if (sc != '{' && sline == 0) {
                                LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Json file must start with {  ");
                                writeHeaderLogOutputFile("Json file must start with {  ");
                                System.exit(ERROR_READING_SOURCE_FILE);
                            }
                            sline++;
    
                            char mc = line.charAt(i);
    
                            if (mc != '}') {
                                jsonLine.append(mc);
                            } else {
                                jsonLine.append(mc);
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Product jsonLine:" + jsonLine + ":");
                                writePSVFile(jsonLine.toString(), countL, 1);
                                jsonLine = new StringBuffer();
                                countL++;
                            }
                        }
                    }
                }
     
                // write json data in psv file for product
                int count = 0;
                int sequence=SEQUENCE;
                int headerLength=destProductFileHeader.split(DELIMITER).length-1;
                
                for(int l=0; l<dataList.size(); l++){
                    if (!dataList.get(l).toString().trim().equals("")) {                   
                        strLine=dataList.get(l).toString().trim();
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
        }
        LOGGER.exiting(CLASSNAME, methodName);
    }
    
    
    
    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
     public void extractJsonAttributeFile(MonitiseEtlHelper monitiseEtlHelper) {
         String methodName = "extractJsonAttributeFile";
         
         String sourceFileName = getSourceFile();
         LOGGER.entering(CLASSNAME, methodName, sourceFileName);
         
         if (sourceFileName != null) {
          
             try {
                String inputFilePath  = sourceFilePath + File.separator + sourceFileName;
                String outputFilePath = destAttributeFilePath + File.separator + destAttributeFilePrefix + getDatePartName(sourceFileName) + FILE_EXTENSION;
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "input file path:" + inputFilePath + " output file path:" + outputFilePath);
                
                File            inputFile = new File(inputFilePath);
                InputStream     fstream   = getFileInputStream(inputFile);
                InputStream     is        = new UnicodeBOMInputStream(fstream).skipBOM();
                BufferedReader  br        = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                
                String strHdrLine;
                String strDataLine;
                String[] hdrArray  = null;
                String[] dataArray = null;
                
                File attrfile = new File(outputFilePath);
                BufferedWriter attrBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(attrfile), "UTF-8"));
                
                // Get JSON Line pipe separated
                int countL = 0;
                String line;
                StringBuffer jsonLine = new StringBuffer();
    
                while ((line = br.readLine()) != null) {
                    
                    line = line.trim();
    
                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "attr line:" + line);
                    
                    if (!line.equals("")) {
                        
                        if (line.charAt(0) != '{') {
                            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Json file must start with {  ");
                            writeHeaderLogOutputFile("Json file must start with {  ");
                            System.exit(ERROR_READING_SOURCE_FILE);
                        }
                        
                        for (int i = 0; i < line.length(); i++) {
    
                            char mc = line.charAt(i);
    
                            if (mc != '}') {
                                jsonLine.append(mc);
                            } else {
                                jsonLine.append(mc);
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Attribute jsonLine:" + jsonLine.toString());
                                writePSVFile(jsonLine.toString(), countL, 2);
                                jsonLine = new StringBuffer();
                                countL++;
                            }
                        }
                    }
                }     
                
                // Get header
                boolean outputHeader = true;
                int sequence=SEQUENCE;
                int headerLength=destProductFileHeader.split(DELIMITER).length-1;
                
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "destProductFileHeader:" + destProductFileHeader);
                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "headerLength:" + headerLength);
                
                HashMap<String, String> mapPT       = new HashMap<String, String>();
                
                for (int l=0; l < attrDataList.size(); l++){
                    if (!attrDataList.get(l).toString().trim().equals("")) { 

                        strHdrLine=attrHdrList.get(l).toString().trim();
                        strDataLine=attrDataList.get(l).toString().trim();
                        
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Countl:" + l + " Header:" + strHdrLine);
                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Countl:" + l + " Data:" + strDataLine);

                        dataArray = strDataLine.split(DELIMITER);
                        hdrArray  = strHdrLine.split(DELIMITER); 
                        
                        if (outputHeader) {                            
                            attrBw.write(destAttrFileHeader);
                            attrBw.newLine();
                            outputHeader=false;
                        }                
    
                        HashMap<String, String> mapPTFields     = new HashMap<String, String>();
                        HashMap<String, String> mapJsonPTFields = new HashMap<String, String>();
                        HashMap<String, String> mapDBPTFields   = new HashMap<String, String>();

                        if (dataArray.length > 2) {
                            
                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "productType:" + dataArray[PRODUCTTYPE]);
                            
                            if (!mapPT.containsKey(dataArray[PRODUCTTYPE])) {
                                String allowableFields = monitiseEtlHelper.getAllowedAttributes(dataArray[PRODUCTTYPE]);
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "allowableFields:" + allowableFields);
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
                                    String jheaderArray=attributeProperty[0];
                                    String jKey="";
                                    for (int i = 0; i < jheaderArray.length(); i++) {
                                        char sc = jheaderArray.charAt(i);
                                        if (i==0){
                                            jKey=jKey+jheaderArray.charAt(i);
                                        }else if(Character.isUpperCase(sc)){
                                            jKey=jKey+jheaderArray.charAt(i);
                                        }
                                    }
                                    jKey=jKey.toLowerCase();
                                    mapJsonPTFields.put(jKey, dataArray[PRODUCTTYPE]);   
                                    mapDBPTFields.put(jKey, attributeProperty[0]);   
                                }       

                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "mapPTFields:" + mapPTFields);
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "mapJsonPTFields:" + mapJsonPTFields);
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "mapDBPTFields:" + mapDBPTFields);
                            }
                        }
                       
                        if (!dataArray[0].equals(FOOTER)) {  

                            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Number of attributes:" + mapJsonPTFields.size());
                            
                            Set<String> keys = mapPTFields.keySet();
                            Iterator<String> keysIt = keys.iterator();
                            
                            while (keysIt.hasNext()) {
                                String ptKey = (String) keysIt.next();
                                String jKey="";
                                for (int i = 0; i < ptKey.length(); i++) {
                                    char sc = ptKey.charAt(i);
                                    if (i==0){
                                        jKey=jKey+ptKey.charAt(i);
                                    }else if(Character.isUpperCase(sc)){
                                        jKey=jKey+ptKey.charAt(i);
                                    }
                                }
                                jKey=jKey.toLowerCase();
                                
                                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "key:" + ptKey + " extractedJsonKey:" + jKey);
                                
                                int jsonPos = -1;

                                for (int i = 26; i < hdrArray.length; i++) {
                                    if (jKey.equals(hdrArray[i])) {
                                        jsonPos = i;
                                        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "jsonPos:" + jsonPos);
                                        i = hdrArray.length;
                                    }
                                }
                                
                                if (jsonPos != -1) {
                                    LOGGER.logp(Level.INFO, CLASSNAME, methodName, "jsonData:" + dataArray[jsonPos]);
                                    
                                    attrBw.write(dataArray[CPCODE] + "|" + dataArray[PRODUCTTYPE] + "|" + dataArray[CPSKU]); 
                                    
                                    attrBw.write( "|" + ptKey);
                                    
                                    attrBw.write( "|" + dataArray[jsonPos]);
                                    
                                    attrBw.write( "|" + sequence);
                                    
                                    attrBw.newLine();
                                }
                            }                                                 
                        } 
                        sequence++;
                    }
                } 
                
                is.close();
                attrBw.close();
            } catch (Exception e) {
                logStackTrace(e, "attribute extracting");
                System.exit(ERROR_READING_SOURCE_FILE);
            }
            archive(sourceFileName);
        }
        LOGGER.exiting(CLASSNAME, methodName);
    }

     /*
      * @param args - output string arguments
      */
     @SuppressWarnings({ "unchecked", "rawtypes" })
    public void writePSVFile(String jsonLine, int lineNumber, int type) {
         String methodName = "writePSVFile";       
         try {
             JSONParser parser = new JSONParser();
             ContainerFactory containerFactory = new ContainerFactory() {
                 
                public List creatArrayContainer() {
                     return new LinkedList();
                 }

                 
                public Map createObjectContainer() {
                     return new LinkedHashMap();
                 }

             };
             
             Map json = (Map) parser.parse(jsonLine, containerFactory);
             LOGGER.logp(Level.INFO, CLASSNAME, methodName, "json map:" + json);
             
             Iterator iter = json.entrySet().iterator();
             StringBuffer jsonKey = new StringBuffer();
             while (iter.hasNext()) {
                 Map.Entry entry = (Map.Entry) iter.next();                   
                 jsonKey.append(entry.getKey() + "|");
             }
             LOGGER.logp(Level.INFO, CLASSNAME, methodName, "jsonKey:" + jsonKey.toString()); 
             if (type == 2) {
                 attrHdrList.add(jsonKey.toString());
             }
             
             if (lineNumber == 0) {                 
                 if (!validateJsonKey(jsonKey.toString())) {
                     LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "The key " + jsonKey.toString()
                             + " are not valid, expecting keys: " + jsonProductHeader + "|" + " in line number " + (lineNumber+1));
                     writeHeaderLogOutputFile("The keys " + jsonKey.toString().substring(0, jsonKey.toString().length()-1) + " are not valid, expecting keys: "
                             + jsonProductHeader+ " in line number " + (lineNumber+1));                   
                     System.exit(INVALID_HEADER);
                 } else {
                     if (type == 1){
                         dataList.add(destProductFileHeader);
                     }
                 }                
             }
             
             Iterator iterr = json.entrySet().iterator();
             StringBuffer rowData     = new StringBuffer();
             StringBuffer jsonKeyData = new StringBuffer();
             
             while (iterr.hasNext()) {
                 Map.Entry entry = (Map.Entry) iterr.next();
                 jsonKeyData.append(entry.getKey() + "|");
                 String value = entry.getValue().toString() + "|";
                 rowData.append(value);                
             }
             LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "rowData:" + rowData.toString());
             
             if (!validateJsonKey(jsonKeyData.toString())) {
                 LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "The key " + jsonKeyData.toString()
                         + " are not valid, expecting keys: " + jsonProductHeader + "|" + " in line number " + (lineNumber+1));
                 writeHeaderLogOutputFile("The keys " + jsonKey.toString().substring(0, jsonKey.toString().length()-1) + " are not valid, expecting keys: " + jsonProductHeader+ " in line number " + (lineNumber+1));
                 System.exit(INVALID_HEADER);
             } else {
                 String data = rowData.toString();
                 data = data.substring(0, data.length()-1);
                 if(type == 2){
                     attrDataList.add(data);
                 }else{
                     dataList.add(data); 
                 }                              
             }             
         } catch (Exception e) {
             logStackTrace(e, "error in writing psv file for json...");

         }
     }
     /**
      * Write the Log Header output file (which is the same as the input file
      * name_header).
      * 
      * @param sourceFile - the source file
      * @param lineCount - the number of lines in the source file
      */
     private void writeHeaderLogOutputFile(String strLine) {
         LOGGER.entering(CLASSNAME, "writeHeaderLogOutputFile", strLine);
         try {
             String sourceFileName = getSourceFile();
             File sourceFile = new File(sourceFilePath + File.separator + LOG_FOLDER + File.separator + HEADER
                     + sourceFileName + LOG_FILE_EXTENSION);
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
     
     /**
      * Archive the source file to the archive folder. Delete the original source
      * file.
      * 
      * @param sourceFileName - the source file name
      */
     private void archive(final String sourceFileName) {
         LOGGER.entering(CLASSNAME, "archive");
         try {
             LOGGER.logp(Level.INFO, CLASSNAME, "archive", "Found Source File:" + sourceFileName);
             File sourceFile = new File(sourceFilePath + File.separator + sourceFileName);
             File archiveFile = new File(sourceFilePath + File.separator + ARCHIVE_FOLDER + File.separator
                     + sourceFileName);
             LOGGER.logp(Level.INFO, CLASSNAME, "archive", "Found Source File:" + sourceFileName);
             FileUtilities.copyFile(sourceFile, archiveFile);
             LOGGER.logp(Level.INFO, CLASSNAME, "archive", "Found Source File:" + sourceFileName);
             sourceFile.delete();
         } catch (IOException e) {
             logStackTrace(e, "archive");
             System.exit(ERROR_WRITING_ARCHIVE_FILE);
         }
         LOGGER.exiting(CLASSNAME, "archive");
     }
     
     private boolean validateJsonKey(final String header) {
         String jsonHeaders = jsonProductHeader + "|";
         return header.trim().contains(jsonHeaders);
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
            && args[DB_PASSWORD].length() > 0
            && args[JSON_PRODUCT_FILE_HEADER].length() > 0) {
            
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating file: " + args[SOURCE_FILE_PATH]);
            GenerateJSONOfferAttributesFile generate = new GenerateJSONOfferAttributesFile(args[SOURCE_FILE_PATH], 
                                                                                           args[SOURCE_FILE_PREFIX], 
                                                                                           args[DEST_PRODUCT_FILE_PATH],
                                                                                           args[DEST_PRODUCT_FILE_PREFIX],
                                                                                           args[DEST_ATTRIBUTE_FILE_PATH], 
                                                                                           args[DEST_ATTRIBUTE_FILE_PREFIX], 
                                                                                           args[DEST_PRODUCT_FILE_HEADER],
                                                                                           args[DEST_ATTR_FILE_HEADER],
                                                                                           args[JSON_PRODUCT_FILE_HEADER]);
   
            try{
                MonitiseEtlHelper monitiseEtlHelper = new MonitiseEtlHelper(args[JDBC_DRIVER], args[DB_URL], args[DB_USERNAME], args[DB_PASSWORD]);
                SEQUENCE=monitiseEtlHelper.getSequence();
                generate.extractJsonProductFile();
                generate.extractJsonAttributeFile(monitiseEtlHelper);
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
             d = fileName.substring(j - DATE_LENGTH, j);
            
        }
        return d;
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

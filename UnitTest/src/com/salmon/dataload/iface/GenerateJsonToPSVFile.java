package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_ARCHIVE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_TARGET_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_HEADER;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.logging.Level;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import com.salmon.dataload.utils.FileUtilities;
import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * Generates the JSON file to pipe seperated file
 * 
 * @author Pranava Mishra
 * @revision : 1.0
 * @Date : 30 July 2014
 */

public final class GenerateJsonToPSVFile {

    private static final String CLASSNAME = GenerateJsonToPSVFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    static final String DELIMITER = "\\|";
    static final String COLUMN_DELIMITER = "|";
    private static final String ARCHIVE_FOLDER = "archive";
    private static final String LOG_FOLDER = "logs";
    private static final String HEADER = "jsonheader_";
    private static final String LOG_FILE_EXTENSION = ".log";
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("ddMMyyyy_hhmmss");
    private static final String FILE_EXTENSION = ".psv";
    
    private static final int SOURCE_FILE_PATH   = 0;
    private static final int SOURCE_FILE_PREFIX = 1;
    private static final int SOURCE_FILE_HEADER = 2;
    private static final int SOURCE_JSON_KEY    = 3;

    private static final int DATE_LENGTH   = 15;
    private static final int NUM_ARGUMENTS = 4;

    private final String sourceFilePath;
    private final String sourceFilePrefix;
    private final String psvFileHeader;
    private final String jsonHeader;
    
    private ArrayList<String> dataList=new ArrayList<String>();

    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param validatedFileName - validated file name
     * @param expectedHeader - the expected header
     */
    public GenerateJsonToPSVFile(final String sourceFilePath, final String sourceFilePrefix,
                                  final String psvFileHeader, final String jsonHeader) {
        this.sourceFilePath   = sourceFilePath;
        this.sourceFilePrefix = sourceFilePrefix;
        this.psvFileHeader    = psvFileHeader;
        this.jsonHeader       = jsonHeader;
    }

    /**
     * Read in the source file, returning an array representation of the
     * contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
    public void extractJsonFile() {
        String methodName = "extractJsonFile";
        LOGGER.entering(CLASSNAME, methodName);

        try {
            String sourceFileName = getSourceFile();
            
            if (sourceFileName != null) {

                LOGGER.logp(Level.INFO, CLASSNAME, methodName, "sourceFileName:" + sourceFileName 
                        + " input file path: " + sourceFilePath + " output file path: " + sourceFilePath);
                
                File            file    = new File(sourceFilePath + File.separator + sourceFileName);
                InputStream     fstream = getFileInputStream(file);
                InputStream     is      = new UnicodeBOMInputStream(fstream).skipBOM();
                BufferedReader  br      = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                
                String psvSourceFileName = sourceFileName;
                psvSourceFileName = psvSourceFileName.replaceFirst(".json", FILE_EXTENSION);
                File psvfile = new File(sourceFilePath + File.separator + psvSourceFileName);
                BufferedWriter psvBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(psvfile), "UTF-8"));
                
                // Get header
                int count = 0;
                String line;
                StringBuffer jsonLine = new StringBuffer();
                int sline = 0;
    
                while ((line = br.readLine()) != null) {
                    line = line.trim();
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
                            // char c = line.charAt(line.length()-1);
                            if (mc != '}') {
                                jsonLine.append(mc);
                            } else {
                                jsonLine.append(mc);
                                writePSVFile(jsonLine.toString(), count);
                                jsonLine = new StringBuffer();
                                count++;
                            }
                        }
                    }
                }
                if(dataList.size() > 0){
                    for(int l=0; l<dataList.size(); l++){
                        psvBw.write(dataList.get(l).toString());
                        psvBw.newLine();
                    }
                }
                try{
                    archive(sourceFileName);
                }catch(Exception ef){
                    logStackTrace(ef, "Error in archiving file...");
                }
                is.close();
                br.close();
                psvBw.close();
            }

        } catch (Exception e) {            
            logStackTrace(e, "Error in json extraction...");
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);
    }

    /*
     * @param args - output string arguments
     */
    public void writePSVFile(String jsonLine, int lineNumber) {
        String methodName = "writePSVFile";
        String sourceFileName = getSourceFile();

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
            Iterator iter = json.entrySet().iterator();
            StringBuffer jsonKey = new StringBuffer();
            if (lineNumber == 0) {
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();                   
                    jsonKey.append(entry.getKey() + "|");
                }
                if (!validateJsonKey(jsonKey.toString())) {
                    LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "The key " + jsonKey.toString()
                            + " are not valid, expecting keys: " + jsonHeader + "|" + " in line number " + (lineNumber+1));
                    writeHeaderLogOutputFile("The keys " + jsonKey.toString().substring(0, jsonKey.toString().length()-1) + " are not valid, expecting keys: "
                            + jsonHeader+ " in line number " + (lineNumber+1));
                    archive(sourceFileName);
                    System.exit(INVALID_HEADER);
                } else {
                    dataList.add(psvFileHeader);                
                }                
            }
            Iterator iterr = json.entrySet().iterator();
            StringBuffer rowData=new StringBuffer();
            StringBuffer jsonKeyData = new StringBuffer();
            while (iterr.hasNext()) {
                Map.Entry entry = (Map.Entry) iterr.next();
                jsonKeyData.append(entry.getKey() + "|");
                String value = entry.getValue().toString() + "|";
                rowData.append(value);                
            }
            if (!validateJsonKey(jsonKeyData.toString())) {
                LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "The key " + jsonKeyData.toString()
                        + " are not valid, expecting keys: " + jsonHeader + "|" + " in line number " + (lineNumber+1));
                writeHeaderLogOutputFile("The keys " + jsonKey.toString().substring(0, jsonKey.toString().length()-1) + " are not valid, expecting keys: " + jsonHeader+ " in line number " + (lineNumber+1));
                archive(sourceFileName);
                System.exit(INVALID_HEADER);
            }else{
                String data=rowData.toString();
                data=data.substring(0, data.length()-1);
                dataList.add(data);               
            }
            
            
            
        } catch (Exception e) {
            logStackTrace(e, "error in writing psv file for json...");

        }
    }

    private boolean validateJsonKey(final String header) {
        String jsonHeaders = jsonHeader + "|";
        return jsonHeaders.equalsIgnoreCase(header.trim());
    }

    /**
     * Two arguments must be required, the source product attributes file name,
     * and the target product attributes file name.
     * 
     * @param args - input arguments
     */
    public static void main(final String[] args) {
        String methodName = "main";
        
        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "arguments passed to GenerateJsonToPSVFile:" 
                + args[SOURCE_FILE_PATH] + "::" 
                + args[SOURCE_FILE_PREFIX] + "::" 
                + args[SOURCE_FILE_HEADER] + "::" 
                + args[SOURCE_JSON_KEY] + "::" );
  
        if (args.length == NUM_ARGUMENTS 
            && args[SOURCE_FILE_PATH].length() > 0 
            && args[SOURCE_FILE_PREFIX].length() > 0 
            && args[SOURCE_FILE_HEADER].length() > 0
            && args[SOURCE_JSON_KEY].length() > 0 ) {
  
            LOGGER.logp(Level.INFO, CLASSNAME, methodName, "Validating file: " + args[SOURCE_FILE_PATH]); 
            
            GenerateJsonToPSVFile generate = new GenerateJsonToPSVFile(args[SOURCE_FILE_PATH],
                                                                       args[SOURCE_FILE_PREFIX],
                                                                       args[SOURCE_FILE_HEADER],
                                                                       args[SOURCE_JSON_KEY]);
            generate.extractJsonFile();
        
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
                // Should change to equals? - we are passing in the full file
                // name for
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
                NavigableSet nset = listOfValidFiles.descendingKeySet();
                Date firstDate = (Date) nset.first();
                LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "firstDate:" + firstDate);
                fileName = listOfValidFiles.get(firstDate);
                LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "fileName:" + fileName);
            } else {
                LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "No files found........");
            }
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
    
    private InputStream getFileInputStream(File sourceFile) throws FileNotFoundException {
        return new FileInputStream(sourceFile);
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

}

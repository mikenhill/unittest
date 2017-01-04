package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_ARCHIVE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;
import static com.salmon.dataload.iface.DataLoadConstants.NO_SOURCE_FILE_FOUND;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.logging.Level;

import com.salmon.dataload.utils.UnicodeBOMInputStream;

/**
 * Class Description
 * @author $User$
 * @date 23 Mar 2012
 * @revision : $Revision$
 */
public final class VolumetricsDepotAttributesFileSplitter {
    
    private static final String CLASSNAME = VolumetricsDepotAttributesFileSplitter.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    
    private static final String TRAILER_DELIMITER = "\\|";
    private static final String FOOTER = "FTRAIL";
    
    private static final String ARCHIVE_FOLDER = "archive";
    private static final int FILE_PATH = 0;
    private static final int FILE_NAME = 1;
    private static final int VALIDATED_DEPOT_ATTR_FILE_NAME = 2;
    private static final int HEADER = 3;

    private static final int NO_OF_PARAMETERS = 4;
    
    private static final int THREE = 3;
    private static final int FOUR = 4;
    private static final int FIVE = 5;
    private static final int TEN_TWENTY_FOUR = 1024;    
    
    private final String sourceFilePath;
    private final String sourceFilePrefix;
    private final String validatedAttributesFileName;
    private final String fileHeader;       
   
    private int lineCount = 0;   
   

    /**
     * Constructor
     * 
     * @param sourceFilePath - source file path
     * @param sourceFilePrefix - source file prefix
     * @param validatedAttributesFileName - validated file to which date attribute values are written
     * @param fileHeader - header used for validation
     */
    private VolumetricsDepotAttributesFileSplitter(final String sourceFilePath, final String sourceFilePrefix, 
            final String validatedAttributesFileName,  final String fileHeader) {

        this.sourceFilePath = sourceFilePath;
        this.sourceFilePrefix = sourceFilePrefix;
        this.validatedAttributesFileName = validatedAttributesFileName;
        this.fileHeader = fileHeader;

    }

    /**
     * Read the input source file. If the trailer count matches the number of records, then output output then write the validated file and archive
     * the source file.
     * 
     * @return The number of records in the source file
     */
    private int run() {
        LOGGER.entering(CLASSNAME, "run");
        
        String sourceFileName = getSourceFile();
        if (sourceFileName == null) {
            System.exit(NO_SOURCE_FILE_FOUND);
        }
                
        LOGGER.logp(Level.INFO, CLASSNAME, "main", "Processing: " + sourceFilePath + File.separator + sourceFileName);
        processFile(sourceFilePath + File.separator + sourceFileName);
        LOGGER.logp(Level.INFO, CLASSNAME, "main", "Archiving: " + sourceFilePath + File.separator + ARCHIVE_FOLDER 
                + File.separator + sourceFileName);
        archive(sourceFilePath, sourceFileName);
        
        LOGGER.exiting(CLASSNAME, "run");
        return lineCount - 1;      

    }
    
    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFileName - the file to be read.
     */
    private void processFile(final String inputFileName) {
        LOGGER.entering(CLASSNAME, "readInputFile", inputFileName);
        
        boolean checkHeader = true;
        try {
            
            FileInputStream fstream = new FileInputStream(inputFileName);
            InputStream is = new UnicodeBOMInputStream(fstream).skipBOM();
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String strLine;
            
            File attrfile = new File(validatedAttributesFileName);
            BufferedWriter attrBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(attrfile), "UTF-8"));
           
            String storeNumber = null;
            
            // Get header
            int count = 0;
            while ((strLine = br.readLine()) != null) {
                if (!strLine.trim().equals("")) {
                    lineCount++;
                    
                    LOGGER.logp(Level.INFO, CLASSNAME, "run", fileHeader + " " + strLine);
                    
                    // write output file and validate header
                    if (checkHeader) {
                        if (fileHeader != null && !fileHeader.equals(strLine)) {
                            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "The file header: {0} does not match the expected header: {1}.", 
                                    new Object[] {
                                    fileHeader, strLine });
                            System.exit(INVALID_ARGUMENTS);
                        }
                        checkHeader = false;
                    }
                    if (count > 0) {
                        
                        if (!strLine.split(TRAILER_DELIMITER)[0].equals(FOOTER)) {
                            LOGGER.logp(Level.INFO, CLASSNAME, "run", strLine);
                            storeNumber = strLine.split(TRAILER_DELIMITER)[0];
                            
                            attrBw.write(storeNumber + "|MaxToteCapping|" + strLine.split(TRAILER_DELIMITER)[1]);
                            attrBw.newLine(); 
                            
                            attrBw.write(storeNumber + "|MaxPayloadCapacity|" + strLine.split(TRAILER_DELIMITER)[2]);
                            attrBw.newLine(); 
                            
                            attrBw.write(storeNumber + "|ToteEquationConstant|" + strLine.split(TRAILER_DELIMITER)[THREE]);
                            attrBw.newLine(); 
                            
                            attrBw.write(storeNumber + "|ToteWeight|" + strLine.split(TRAILER_DELIMITER)[FOUR]);
                            attrBw.newLine(); 
                            
                            attrBw.write(storeNumber + "|PredictedWeight|" + strLine.split(TRAILER_DELIMITER)[FIVE]);
                            attrBw.newLine();   
                        } 
                        
                    } else {
                        attrBw.write("DEPOT_NUMBER|NAME|VALUE");
                        attrBw.newLine();
                        count++;
                    }
                } 
            }
                        
            is.close();
            attrBw.close();
        } catch (Exception e) {
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, "readInputFile");
    }
    
    /**
     * Archive the source file to the archive folder. Delete the original source file.
     * 
     * @param filePath - the source file path
     * @param fileName - the archive file name
     */
    private void archive(final String filePath, final String fileName) {
        LOGGER.entering(CLASSNAME, "archive");
        InputStream inStream = null;
        OutputStream outStream = null;
        
        try {
            File archiveFile = new File(sourceFilePath + File.separator + ARCHIVE_FOLDER + File.separator + fileName);
            File sourceFile = new File(sourceFilePath + File.separator + fileName);
            
            inStream = new FileInputStream(sourceFile);
            outStream = new FileOutputStream(archiveFile);
 
            byte[] buffer = new byte[TEN_TWENTY_FOUR];
 
            int length;
            //copy the file content in bytes 
            while ((length = inStream.read(buffer)) > 0) { 
                outStream.write(buffer, 0, length); 
            }
 
            inStream.close();
            outStream.close();
 
            //delete the original file
            sourceFile.delete();
            
        } catch (Exception e) {
            System.exit(ERROR_WRITING_ARCHIVE_FILE);
        }
        LOGGER.exiting(CLASSNAME, "archive");
    }

    /**
     * Return a matching file for the given source file path and prefix.
     * 
     * @return the source file name.
     */
    private String getSourceFile() {
        File dir = new File(sourceFilePath);
        String[] files = dir.list();
        if (files != null) {
            for (String file : files) {
                if (file.startsWith(sourceFilePrefix)) {
                    LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "Found Source File:" + file);
                    return file;
                }
            }
        }
        return null;
    }

    /**
     * Three arguments must be passed representing the source file path, the source file prefex and the validated file name.
     * 
     * @param args - input parameters
     */
    public static void main(final String[] args) {
        VolumetricsDepotAttributesFileSplitter validate = null;

        if (args.length == NO_OF_PARAMETERS && args[FILE_PATH].length() > 0 && args[FILE_NAME].length() > 0
                && args[VALIDATED_DEPOT_ATTR_FILE_NAME].length() > 0 && args[HEADER].length() > 0) {

            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Loading file: " + args[FILE_PATH] + System.getProperty("file.separator") + args[FILE_NAME]);
            validate = new VolumetricsDepotAttributesFileSplitter(args[FILE_PATH], args[FILE_NAME], 
                    args[VALIDATED_DEPOT_ATTR_FILE_NAME],  args[HEADER]);
            validate.run();
        } else {

            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments in VolumetricsDepotAttributesFileSplitter");
            System.exit(INVALID_ARGUMENTS);
        }
    }
}


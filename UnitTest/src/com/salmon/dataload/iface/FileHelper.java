package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_READING_SOURCE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_ARCHIVE_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.ERROR_WRITING_TARGET_FILE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Utility helper class allowing manipulation of files.
 * @author Nadim Atta
 *
 */
public final class FileHelper {

    private static final String CLASSNAME = FileHelper.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);
    
    /**
     * Private constructor
     */
    private FileHelper() { }

    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param dir - directory to be read.
     * @param inputFileName - the file to be read.
     * @return an array representation of the source file.
     */
    public static ArrayList<String> readInputFile(final String dir, final String inputFileName) {
        String methodName = "readInputFile";
        LOGGER.entering(CLASSNAME, "readInputFile", inputFileName);
        ArrayList<String> lines = null;
        try {
            FileInputStream fstream = new FileInputStream(dir + File.separator + inputFileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            lines = new ArrayList<String>();
            while ((strLine = br.readLine()) != null) {
                lines.add(strLine);
            }
            in.close();
        } catch (Exception e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "processInputFile", "Error reading input files.", e);
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);
        return lines;
    }

    /**
     * Return a matching file for the given source file path and prefix.
     * 
     * @param sourceFilePath file name path
     * @param sourceFilePrefix source filename prefix
     * @return the source file name.
     * 
     */
    public static String getSourceFile(final String sourceFilePath, final String sourceFilePrefix) {
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
     * Write the validated output file (which is the same as the input file without the trailer).
     * 
     * @param fileName Name of file to write to.
     * @param lines An array representation of the source file
     */
    public static void writeOutputFile(final List<String> lines, final String fileName) {
        String methodName = "writeOutputFile";
        LOGGER.entering(CLASSNAME, methodName, fileName);
        LOGGER.logp(Level.INFO, CLASSNAME, "getSourceFile", "Validated filename:" + fileName);

        try {
            File file = new File(fileName);
            BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                bw.write(line);
                bw.newLine();
            }
            bw.close();
        } catch (Exception e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Error creating output file.", e);
            System.exit(ERROR_WRITING_TARGET_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);

    }

    /**
     * Archive the source file to the archive folder. Delete the original source file.
     * 
     * @param lines - an array representation of the source file
     * @param filePath - the source file path
     * @param fileName - the archive file name
     * @param archiveFolder The name of the archive folder.
     */
    public static void archive(final List<String> lines, final String filePath, final String fileName, final String archiveFolder) {
        String methodName = "archive";
        LOGGER.entering(CLASSNAME, "archive");
        try {
            File file = new File(filePath + File.separator + archiveFolder + File.separator + fileName);
            BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
            for (String line : lines) {
                bw.write(line);
                bw.newLine();
            }
            bw.close();
            File sourceFile = new File(filePath + File.separator + fileName);
            sourceFile.delete();
        } catch (Exception e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Error archiving file.", e);
            System.exit(ERROR_WRITING_ARCHIVE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);
    }

    /**
     * Get the matching file that start with the entered name.
     * 
     * @param dir Directory of file.
     * @param fileName starting name of file.
     * @return Matching file name.
     */
    public static String getMatchingFile(final String dir, final String fileName) {
        LOGGER.entering(CLASSNAME, "getMatchingFile");
        String matchingFile = null;
        File inputDirectory = new File(dir);
        
        if (!inputDirectory.exists()) {
            return null;
        }
        
        String[] files = inputDirectory.list();

        for (String currentFile : files) {
            if (currentFile.startsWith(fileName)) {
                matchingFile = currentFile;
                break;
            }
        }
        LOGGER.exiting(CLASSNAME, "getMatchingFile");
        return matchingFile;
    }  

    /**
     * Read in the source file, returning an array representation of the contents of the file.
     * 
     * @param inputFile - the file to be read.
     * @return an array representation of the source file.
     */
    public static ArrayList<String> readInputFile(final File inputFile) {
        String methodName = "readInputFile";
        LOGGER.entering(CLASSNAME, methodName, inputFile);
        ArrayList<String> lines = null;
        try {
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            lines = new ArrayList<String>();
            while ((strLine = br.readLine()) != null) {
                lines.add(strLine);
            }
            in.close();
        } catch (Exception e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Error reading input files.", e);
            System.exit(ERROR_READING_SOURCE_FILE);
        }
        LOGGER.exiting(CLASSNAME, methodName);
        return lines;
    }

}

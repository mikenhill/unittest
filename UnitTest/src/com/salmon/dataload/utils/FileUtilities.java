package com.salmon.dataload.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class Description: File handling utilities
 * 
 * @author Ashish Agrawal
 * @date 18 Jan 2012
 * @revision : 1.0
 */
public final class FileUtilities {

    private static final String CLASSNAME = FileUtilities.class.getName();
    private static final Logger LOGGER = Logger.getLogger(CLASSNAME);

    private static final int LINE_BREAK_0X_D = 0xD;
    private static final int LINE_BREAK_0X_A = 0xA;
    private static final int DEFAULT_LINE_COUNT = 0;
    private static final int BUFFER_SIZE = 4096;

    /**
     * Private Constructor
     */
    private FileUtilities() {
    }

    /**
     * Description: Read the last line very quickly, by starting at the end of the file and reading backwards to the first carriage return characters.
     * 
     * @param fileName String
     * @return String
     */
    public static String getLastLine(final String fileName) {
        return getLastLine(new File(fileName));
    }

    /**
     * Description: Read the last line very quickly, by starting at the end of the file and reading backwards to the first carriage return characters.
     * 
     * @param file File
     * @return String
     */
    public static String getLastLine(final File file) {
        String methodName = "getLastLine";
        LOGGER.entering(CLASSNAME, methodName, new Object[] { file });

LOGGER.logp(Level.INFO, CLASSNAME, methodName, "file:" + file.getAbsolutePath());
        String lastLine = null;

        try {
            RandomAccessFile fileHandler = new RandomAccessFile(file, "r");
            long fileLength = file.length() - 1;
            StringBuilder sb = new StringBuilder();
LOGGER.logp(Level.INFO, CLASSNAME, methodName, "fileLength:" + fileLength);

            for (long filePointer = fileLength; filePointer != -1; filePointer--) {
                fileHandler.seek(filePointer);
                int readByte = fileHandler.readByte();

                if (readByte == LINE_BREAK_0X_A) {
                    if (filePointer == fileLength) {
                        continue;
                    } else {
                        break;
                    }
                } else if (readByte == LINE_BREAK_0X_D) {
                    if (filePointer == fileLength - 1) {
                        continue;
                    } else {
                        break;
                    }
                }

                sb.append((char) readByte);
            }

            lastLine = sb.reverse().toString();
            fileHandler.close();

        } catch (Exception e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Exception caught", e);
        }
LOGGER.logp(Level.INFO, CLASSNAME, methodName, "lastLine:" + lastLine);
        LOGGER.exiting(CLASSNAME, methodName, new Object[] { lastLine });
        return lastLine;
    }

    /**
     * This method returns the total number of lines in the file.
     * 
     * @param inputFile File
     * @return Long total number of lines
     */
    public static long getTotalLinesCount(final File inputFile) {
        String methodName = "getTotalLinesCount";
        LOGGER.entering(CLASSNAME, methodName, new Object[] { inputFile });
        long totalLinesCount = Long.valueOf(DEFAULT_LINE_COUNT);
        try {
            LineNumberReader reader = new LineNumberReader(new FileReader(inputFile));
            while (reader.readLine() != null) {
                continue;
            }
            totalLinesCount = reader.getLineNumber();
            reader.close();
        } catch (Exception e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Exception caught", e);
        }

        LOGGER.exiting(CLASSNAME, methodName, new Object[] { totalLinesCount });
        return totalLinesCount;
    }
    
    /**
     * This method returns the total number of lines without a empty row in the file.
     * 
     * @param inputFile File
     * @return Long total number of lines
     */
    public static long getTotalLinesWithoutSpaceCount(final File inputFile) {
        String methodName = "getTotalLinesCount";
        LOGGER.entering(CLASSNAME, methodName, new Object[] { inputFile });
        long totalLinesCount = Long.valueOf(DEFAULT_LINE_COUNT);
        try {
            int line=0;
            FileInputStream fstream = new FileInputStream(inputFile);
            InputStream is = new UnicodeBOMInputStream(fstream).skipBOM();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String strLine;            
            while ((strLine = reader.readLine()) != null) {
                if(!strLine.trim().equals("")){
                    line++;
                }
                continue;
            }
            totalLinesCount = line;
            reader.close();
            is.close();
            fstream.close();
        } catch (Exception e) {
            LOGGER.logp(Level.SEVERE, CLASSNAME, methodName, "Exception caught", e);
        }

        LOGGER.logp(Level.INFO, CLASSNAME, methodName, "totalLinesCount:" + totalLinesCount);
        LOGGER.exiting(CLASSNAME, methodName, new Object[] { totalLinesCount });
        return totalLinesCount;
    }

    /**
     * Copies a file from the source to the destination
     * 
     * @param fileSource - source file
     * @param fileDestination - destination file
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void copyFile(final File fileSource, final File fileDestination) throws IOException {
        InputStream inputstream = new FileInputStream(fileSource);
        OutputStream outputstream = new FileOutputStream(fileDestination);

        byte[] abBuffer = new byte[BUFFER_SIZE];
        while (true) {
            int nBytesRead = inputstream.read(abBuffer);

            if (nBytesRead <= 0) {
                break;
            }
            outputstream.write(abBuffer, 0, nBytesRead);

        }
        inputstream.close();
        outputstream.close();
    }

}

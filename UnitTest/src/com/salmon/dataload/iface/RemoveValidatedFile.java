package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_DELETING_VALIDATED_FILE;
import static com.salmon.dataload.iface.DataLoadConstants.INVALID_ARGUMENTS;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.logging.Level;

/**
 * This class is responsible deleting the validated load file(s) from the the interface directory after it has been loaded.
 * 
 * @author Stephen Gair
 * @revision : 1.0
 * @Date : 19 December 2011
 */
public final class RemoveValidatedFile {

    private static final String CLASSNAME = RemoveValidatedFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);

    /**
     * Constructor
     */
    private RemoveValidatedFile() {
    }

    /**
     * Main is called with a list of files to be deleted.
     * 
     * @param args - input arguments
     */
    public static void main(final String[] args) {
        if (args.length > 0) {
            LOGGER.logp(Level.INFO, CLASSNAME, "main", "Removing validated files: " + args.length);
            RemoveValidatedFile validate = new RemoveValidatedFile();
            validate.deleteFiles(args);
        } else {
            LOGGER.logp(Level.SEVERE, CLASSNAME, "main", "Invalid arguments in RemoveValidatedFile");
            System.exit(INVALID_ARGUMENTS);
        }
    }

    /**
     * Deletes a set of files
     * 
     * @param fileNames - a list of file names to delete.
     * @throws SecurityException
     */
    private void deleteFiles(final String... fileNames) {
        LOGGER.entering(CLASSNAME, "deleteFiles", fileNames);
        for (String fileName : fileNames) {
            File file = new File(fileName);
            try {
                if (file.exists()) {
                    file.delete();
                }
            } catch (SecurityException se) {
                logStackTrace(se, "deleteFiles");
                System.exit(ERROR_DELETING_VALIDATED_FILE);
            }
            LOGGER.logp(Level.INFO, CLASSNAME, "deleteFiles", fileName);
        }
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

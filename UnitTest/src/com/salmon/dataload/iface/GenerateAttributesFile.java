package com.salmon.dataload.iface;

import static com.salmon.dataload.iface.DataLoadConstants.ERROR_GENERATING_PRODUCT_ATTRIBUTES_FILE;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.logging.Level;

/**
 * Abstract class that contains methods to allow the generation of the attributes file for the products and pricing interfaces.
 * 
 * @author Stephen Gair
 * @Date : 19 December 2011
 */

public abstract class GenerateAttributesFile {

    private static final String TARGET_FILE_HEADER = "Identifier|Type|Displayable|Searchable|Comparable|Name|Description|AllowedValue1|Delete";
    private static final String TARGET_FILE_LINE = "$Identifier|STRING|TRUE|TRUE|TRUE|$Name|$Description|$AllowedValue|";
    static final String DELIMITER = "\\|";

    private final String sourceFileName;
    private final String targetFileName;
    private static final String CLASSNAME = GenerateAttributesFile.class.getName();
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(CLASSNAME);

    /**
     * Constructor
     * 
     * @param sourceFileName - the source file name
     * @param targetFileName - the target file name
     */
    GenerateAttributesFile(final String sourceFileName, final String targetFileName) {
        this.sourceFileName = sourceFileName;
        this.targetFileName = targetFileName;
    }

    /**
     * Read the input file and write the product attributes output file.
     */
    void run() {
        try {
            List<Attribute> attributes = readInputFile();
            writeOutputFile(attributes);
        } catch (FileNotFoundException fnfe) {
            logStackTrace(fnfe, "run");
            System.exit(ERROR_GENERATING_PRODUCT_ATTRIBUTES_FILE);
        } catch (SecurityException se) {
            logStackTrace(se, "run");
            System.exit(ERROR_GENERATING_PRODUCT_ATTRIBUTES_FILE);
        } catch (IOException ioe) {
            logStackTrace(ioe, "run");
            System.exit(ERROR_GENERATING_PRODUCT_ATTRIBUTES_FILE);
        }
    }

    /**
     * Read the source import file and return an ArrayList of Attributes which hold the identifier and values.
     * 
     * @return an array list representation of the input file.
     * @throws FileNotFoundException
     * @throws SecurityException
     * @throws IOException
     */
    abstract List<Attribute> readInputFile() throws IOException;

    /**
     * Write out the attribute import file based on the list of identifiers and values.
     * 
     * @param attributes - an array list of Attributes.
     * @throws IOException
     */
    private void writeOutputFile(final List<Attribute> attributes) throws IOException {
        LOGGER.entering(CLASSNAME, "writeOutputFile", targetFileName);
        File file = new File(targetFileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
        bw.write(TARGET_FILE_HEADER);
        for (Attribute a : attributes) {
            bw.newLine();
            String line = TARGET_FILE_LINE;
            line = line.replace("$Identifier", a.id);
            line = line.replace("$Name", a.id);
            line = line.replace("$Description", "Description of " + a.id);
            line = line.replace("$AllowedValue", a.value);
            bw.write(line);
        }
        bw.close();
        LOGGER.exiting(CLASSNAME, "writeOutputFile");
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

    String getSourceFileName() {
        return sourceFileName;
    }

    /**
     * Attribute helper class
     * 
     */
    class Attribute {
        private final String id, value;

        /**
         * Constructor
         * 
         * @param i - id
         * @param v - value
         */
        Attribute(final String i, final String v) {
            id = i;
            value = v;
        }
    }
}

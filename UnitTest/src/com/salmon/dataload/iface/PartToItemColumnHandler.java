package com.salmon.dataload.iface;

import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.commerce.foundation.dataload.config.AbstractColumnHandler;
import com.ibm.commerce.foundation.dataload.exception.DataLoadException;
import com.ibm.commerce.foundation.dataload.object.ExtendedTableDataObject;

/**
 * Class Description
 * 
 * @author $User$
 * @date 7 Mar 2012
 * @revision : $Revision$
 */
public class PartToItemColumnHandler extends AbstractColumnHandler {

    private static final String CLASSNAME = PartToItemColumnHandler.class.getName();
    private static final Logger LOGGER = Logger.getLogger(CLASSNAME);

    /**
     * This function returns a string item from a part number
     * 
     * @param paramMap contains one columnName,columnValue pair. An exception is thrown if a column value other than Y,N,T or F is found.
     * @param paramExtendedTableDataObject Not used
     * @return String containing '0' or '1'.
     */
    public String resolveColumnValue(final Map<String, String> paramMap, final ExtendedTableDataObject paramExtendedTableDataObject)
            throws DataLoadException {
        final String methodName = "resolveColumnValue";
        LOGGER.entering(CLASSNAME, methodName);

        String retVal = "";

        // We expect only one item in paramMap
        Collection<String> keys = paramMap.keySet();
        if (keys.size() < 1 || keys.size() > 1) {
            String msg = "Unexpected number of items, " + Integer.toString(keys.size()) + ". Only 1 item was expected.";
            LOGGER.logp(Level.SEVERE, CLASSNAME, "resolveColumnValue", msg);
            LOGGER.throwing(CLASSNAME, methodName, new DataLoadException(msg));
        }

        // As expected, there is only one item paramMap - we can now validate
        // the value of the item and return the corresponding number value
        String key = (String) keys.toArray()[0];
        String strValue = paramMap.get(key);

        LOGGER.logp(Level.INFO, CLASSNAME, "resolveColumnValue", "Found ColumnName=" + key + ",ColumnValue=" + strValue);

        if (strValue == null) {
            String msg = "Unallowable value, " + strValue + ", for column " + key + " was detected.";
            LOGGER.logp(Level.SEVERE, CLASSNAME, "resolveColumnValue", msg);
            LOGGER.throwing(CLASSNAME, methodName, new DataLoadException(msg));
        }

        LOGGER.exiting(CLASSNAME, methodName);

        return strValue.substring(0, strValue.length() - 2);
    }
}

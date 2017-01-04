package com.salmon.training.unittest;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.commerce.command.ControllerCommandImpl;
import com.ibm.commerce.exception.ECException;
import com.salmon.commerce.commonregistry.utils.CommonRegistryHelper;
import com.salmon.commerce.lowstock.dao.LowStockDao;
import com.salmon.commerce.lowstock.dto.LowAtpInventoryDto;
import com.salmon.commerce.lowstock.dto.LowStockDto;
import com.salmon.common.SalmonConstants;
import com.salmon.common.utils.SchedulerHelper;

public class AdvancedCmdImpl extends ControllerCommandImpl  {
	
	private static final long serialVersionUID = 1L;
    private static final String CLASSNAME = AdvancedCmdImpl.class.getName();
    private static final Logger LOGGER = Logger.getLogger(CLASSNAME);
    private static int  DEFAULT_LOW_STOCK_THRESHOLD = 100;
    private LowStockDao lowStockDao = null;
    private int  intToBeSet = 33;
    
    public int getIntToBeSet() {
		return intToBeSet;
	}

	public void setIntToBeSet(int intToBeSet) {
		this.intToBeSet = intToBeSet;
	}

	public int getDefaultLowStockThreshold() {
		return DEFAULT_LOW_STOCK_THRESHOLD;
	}
	
	public LowStockDao getLowStockDao() {
		if (lowStockDao == null) {
			lowStockDao = new LowStockDao();
		}
		return lowStockDao;
	}



	public void setLowStockDao(LowStockDao lowStockDao) {
		this.lowStockDao = lowStockDao;
	}

	public Float getSquare (final Float toBeSquared) {
		return new Float (toBeSquared * toBeSquared);
	}
	
	public static Float getCube (final Float toBeCubed) {
		return new Float (toBeCubed * toBeCubed * toBeCubed);
	}

	private Float getHalf (final Float toBeHalved) {
		return new Float (toBeHalved/2);
	}
	
	public List<LowStockDto> setAnything () {
		LowStockDao theDao = new LowStockDao();
		return theDao.getAllLowStockRecords();
	}
	
	
	@Override    
    public void performExecute() throws ECException {
        String methodName = "performExecute";
        LOGGER.entering(CLASSNAME, methodName);
        
        //Ensure a job is not already running
        if (SchedulerHelper.isJobRunning(SalmonConstants.LS_SCHEDULED_JOB_PATH_INFO)) {
        	LOGGER.logp(Level.WARNING, CLASSNAME, methodName, "Exiting as job already running");
        	return;
        }
        
        //Step 0 - Check that low stock checking is active
        String lowStockChecking = CommonRegistryHelper.getRegistryValue(getCommandStoreId(), 
        		"LS_LOW_STOCK_CHECKS_ACTIVE", "N");
        
        if ("Y".equals(lowStockChecking)) {
        
        	int lowStockThresholdInt = getLowStockThreshold();
        	
        	List<String> listDoNotCheckPartnumbers = getDoNotCheckPartnumbers();
        	
	        //Step 1 - invoke the DAO to get back all low stock items
	        List<LowAtpInventoryDto> listLowAtpInventoryDto = getLowStockDao().getLowStockItems(lowStockThresholdInt, getCommandStoreId());
	        if (listLowAtpInventoryDto != null && listLowAtpInventoryDto.size() > 0) {
	        	for (LowAtpInventoryDto lowAtpInventoryDto : listLowAtpInventoryDto) {
	        		if (!listDoNotCheckPartnumbers.contains(lowAtpInventoryDto.getPartnumber())) {
	        			getLowStockDao().addLowStockAlertRecord(lowAtpInventoryDto);
	        		}
	        	}
	        }
	        
	        //Step 2 - invoke the DAO to get back all low stock items alerted items that are now no longer low stock
	        List<LowAtpInventoryDto> listLowNowHighAtpInventoryDto = getLowStockDao().getHighStockAlertedItems(lowStockThresholdInt, getCommandStoreId());	        
	        if (listLowNowHighAtpInventoryDto != null && listLowNowHighAtpInventoryDto.size() > 0) {
	        	for (LowAtpInventoryDto lowNowHighAtpInventoryDto : listLowNowHighAtpInventoryDto) {
	        		if (!listDoNotCheckPartnumbers.contains(lowNowHighAtpInventoryDto.getPartnumber())) {
	        			getLowStockDao().removeLowStockAlertRecord(lowNowHighAtpInventoryDto.getCatentryId());
	        		}
	        	}
	        }        
        }
        LOGGER.exiting(CLASSNAME, methodName);
	}

	private int getLowStockThreshold () {
		String methodName = "getLowStockThreshold";
        LOGGER.entering(CLASSNAME, methodName);
        int lowStockThresholdInt = 0;
    	String lowStockThreshold = "not set";
    	try {
        	lowStockThreshold = CommonRegistryHelper.getRegistryValue(getCommandStoreId(), 
            		"LS_LOW_STOCK_THRESHOLD");        	
        	lowStockThresholdInt = new Integer(lowStockThreshold).intValue();
        	LOGGER.logp(Level.FINE, CLASSNAME, methodName, "Low stock threshold is set = " + lowStockThreshold);
        	
    	} catch (Exception ex) {
    		LOGGER.logp(Level.WARNING, CLASSNAME, methodName, "Low stock threshold is not set or invalid = " + lowStockThreshold 
    				+ " using default of " + DEFAULT_LOW_STOCK_THRESHOLD);
    		lowStockThresholdInt = DEFAULT_LOW_STOCK_THRESHOLD;
    	}
        
        LOGGER.exiting(CLASSNAME, methodName);
        return lowStockThresholdInt;
	}
	
	
	private List<String> getDoNotCheckPartnumbers () {
		String methodName = "getDoNotCheckPartnumbers";
        LOGGER.entering(CLASSNAME, methodName);
        
        List<String> listDoNotCheckPartnumbers = new ArrayList<String> ();
    	
    	try {
    		listDoNotCheckPartnumbers = CommonRegistryHelper.getRegistryListValues(getCommandStoreId(), 
            		"LS_LOW_STOCK_EXCLUDED_PT_NUMBERS");
        	
        	LOGGER.logp(Level.FINE, CLASSNAME, methodName, "list of Do Not Check Partnumbers = " + listDoNotCheckPartnumbers);
        	
    	} catch (Exception ex) {
    		LOGGER.logp(Level.WARNING, CLASSNAME, methodName, "list of Do Not Check Partnumbers is not set or invalid = " + listDoNotCheckPartnumbers );

    	}
        
        LOGGER.exiting(CLASSNAME, methodName);
        return listDoNotCheckPartnumbers;
	}
	
}

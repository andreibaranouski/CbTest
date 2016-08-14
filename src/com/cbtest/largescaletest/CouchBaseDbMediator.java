package com.cbtest.largescaletest;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;

import org.apache.log4j.Logger;
import org.ini4j.Options;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.google.gson.Gson;
import com.cbtest.largescaletest.iteminfo.AbContact;
import com.cbtest.largescaletest.iteminfo.AddressBook;
import com.cbtest.largescaletest.iteminfo.CallInfo;
import com.cbtest.largescaletest.iteminfo.Contact;
import com.cbtest.largescaletest.iteminfo.DeviceInfo;
import com.cbtest.largescaletest.iteminfo.MessageInfo;

public class CouchBaseDbMediator extends DbMediator {
	private static final int MAX_LIST_REMOVALS_BEFORE_COMPRESS = 10;
	private static final String LIST_DELIMITER = ",";
	private static final char REMOVE_TOKEN = '-';
	private static final String AB_CONTACT_DELIMITER = "~";
	private static final int CB_OP_TIMEOUT = 5000;
	protected static Logger logger = Logger.getLogger(CouchBaseDbMediator.class);
	private CouchbaseClient[] client = new CouchbaseClient[BucketName.values().length];
	private ABtype abtype;
	private Gson gson = new Gson();
	private Random random = new Random();
	
	public enum ABtype {
		SINGLE_RECORD(1),
		TEN_RECORDS(10),
		HUNDRED_RECORDS(100),
		;
		
		int size;
		private ABtype(int size) {
			this.size = size;
		}
	}

	@Override
	public void connectToDb() throws CbTestDbException {
		String dbUri = "http://127.0.0.1:8091/pools";
		try {
			Options opt = new Options(new FileReader("dbtest.ini"));
			dbUri = opt.get("CouchBaseURI");
			abtype = ABtype.valueOf(opt.get("CouchBaseABtype", "TEN_RECORDS"));
		} catch ( IOException e) {
			throw new CbTestDbException("Couldn't read CouchBase ini file: " + e.getMessage());
		}
		
		 // Set the URIs and get a client
	    List<URI> uris = new LinkedList<URI>();
	
	    // Connect to localhost or to the appropriate URI
	    uris.add(URI.create(dbUri));
	    
	    CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        cfb.setOpTimeout(5000);	// set default timeout to 5 seconds

	    try {
	    	for (BucketName bucket : BucketName.values()) { 
	    	    CouchbaseConnectionFactory cf = cfb.buildCouchbaseConnection(uris, bucket.name(), "", "");
	    		client[bucket.ordinal()] = new CouchbaseClient(cf);
	    	}
	    } catch (Exception e) {
	    	logger.error("Error connecting to Couchbase: " + e.getMessage());
	    	throw new CbTestDbException("Error connecting to Couchbase: " + e.getMessage());
	    }
	}

	@Override
	public void disconnectDb() {
		// Shutdown the client
		for (BucketName bucket : BucketName.values())
			client[bucket.ordinal()].shutdown(3, TimeUnit.SECONDS);
	}

	@Override
	public long getNumRecords() throws CbTestDbException {
		long numRecords = 0;
		for (BucketName bucket : BucketName.values()) {
			String result = null;
			for (Map<String, String> stats : client[bucket.ordinal()].getStats().values())
			{
				result = stats.get("ep_total_enqueued");
				if (result == null)
					result = stats.get("total_items");			
			}
			if (result == null)
				throw new CbTestDbException("Couldn't access CouchBase server");
			try {
				numRecords += new Long(result);
			} catch (NumberFormatException e) {
				logger.error("Error parsing number of records result - " + result);
			}
		}
		return numRecords;
	}
	
	protected void addItem(String key, String value, int expiry, Table table, boolean throwExceptionIfExists) throws ViberDbItemExists, CbTestDbException {
		boolean success;
		try {
			success = client[table.bucket.ordinal()].add(table.prefix + key, expiry, value).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
		} catch (Exception e) {
			throw new CbTestDbException("Error adding " + table.prefix + key + " - " + e.toString());
		}
		if (throwExceptionIfExists && !success)
			throw new ViberDbItemExists(table.prefix + key + " already exists");
	}

	protected void removeItem(String key, Table table, boolean throwExceptionIfNotFound) throws ViberDbItemNotFound, CbTestDbException {
		boolean success;
		try {
			success = client[table.bucket.ordinal()].delete(table.prefix + key).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();				
		} catch (Exception e) {
			throw new CbTestDbException("Error removing " + table.prefix + key + " - " + e.toString());
		}
		if (throwExceptionIfNotFound && !success)
			throw new ViberDbItemNotFound(table.prefix + key + " not found");
	}
	
	public String getItem(String key, Table table, boolean throwExceptionIfNotFound) throws ViberDbItemNotFound, CbTestDbException {
		String result;
		try {
			result = (String)client[table.bucket.ordinal()].get(table.prefix + key);
		} catch (Exception e) {
			throw new CbTestDbException("Error getting " + table.prefix + key + " - " + e.toString());
		}
		if (throwExceptionIfNotFound && result == null)
			throw new ViberDbItemNotFound(table.prefix + key + " not found");
		return result;
	}
	
	public Map<String,Object> getItems(Set<String> keys, Table table, String prefix, boolean throwExceptionIfNotFound) throws ViberDbItemNotFound, CbTestDbException {
		// get multiple items from DB
		Map<String,Object> result = null;
		prefix = table.prefix + prefix;
		try {
			Set<String> keysWithPrefix;
			if (prefix.isEmpty())
				keysWithPrefix = keys;
			else {
				keysWithPrefix = new HashSet<String>();
				for (String key : keys)
					keysWithPrefix.add(prefix + key);
			}
			
			result = client[table.bucket.ordinal()].getBulk(keysWithPrefix);	// get all values at once
		} catch (Exception e) {
			throw new CbTestDbException("Error getting keys for " + prefix + " keys: " + keys.toString() + " - " + e.toString());
		}
		if (result == null)
			throw new CbTestDbException("Error getting keys for " + prefix + " keys: " + keys.toString());
		
		if (throwExceptionIfNotFound && result.size() == keys.size()) 
			throw new ViberDbItemNotFound("Not all keys for " + prefix + " were found. Requested: " + keys.toString() + ", received: " + result.keySet().toString());
		
		if (prefix.isEmpty())
			return result;
		
		// remove table.prefix from key result
		int prefixLen = prefix.length();
		Map<String, Object> itemMap = new HashMap<String, Object>();
		for (Entry<String, Object> item : result.entrySet())
			itemMap.put(item.getKey().substring(prefixLen), item.getValue());
		return itemMap;
	}
	
	@SuppressWarnings("unchecked")
	public void removeItems(Set<String> keys, Table table, String prefix, boolean throwExceptionIfNotFound) throws ViberDbItemNotFound, CbTestDbException {
		boolean success = true;
		OperationFuture<Boolean>[] futureOps = new OperationFuture[keys.size()];
		try {
			int i = 0;
			for (String key : keys)
				futureOps[i++] = client[table.bucket.ordinal()].delete(table.prefix + prefix + key);

			for (OperationFuture<Boolean> op : futureOps)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue())
					success = false;
		} catch (Exception e) {
			throw new CbTestDbException("Error removing keys for " + table.prefix + prefix + " keys: " + keys.toString() + " - " + e.toString());
		}
		
		if (throwExceptionIfNotFound && !success) 
			throw new ViberDbItemNotFound("Not all keys for " + table.prefix + prefix + " were found, keys:: " + keys.toString());
	}
	
	public void updateItem(String key, String value, Table table, boolean throwExceptionIfNotFound) throws ViberDbItemNotFound, CbTestDbException {
		boolean success;
		try {
			success = client[table.bucket.ordinal()].replace(table.prefix + key, 0, value).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
		} catch (Exception e) {
			throw new CbTestDbException("Error updating" + table.prefix + key + " - " + e.toString());
		}
		if (!success)
			throw new ViberDbItemNotFound(table.prefix + key + " not found");
	}

	protected void addItemToDelimitedList(String key, String itemToAdd, Table table) throws CbTestDbException {
		try {
			// 1st try to append value
			boolean success = client[table.bucket.ordinal()].append(0, table.prefix + key, LIST_DELIMITER + itemToAdd).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
			if (!success) {
				// if value doesn't exist, then add a new value
				success = client[table.bucket.ordinal()].add(table.prefix + key, 0, LIST_DELIMITER + itemToAdd).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
				
				if (!success) {
					// if value now exists (someone beat us to the add), then append again
					success = client[table.bucket.ordinal()].append(0, table.prefix + key, LIST_DELIMITER + itemToAdd).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
					
					if (!success)
						throw new CbTestDbException("Tried appending, adding and appending again - but failed...");
				}
			}
		} catch (Exception e) {
			throw new CbTestDbException("Error adding item " + itemToAdd + " to " + table.prefix + " list of " + key + " - " + e.toString());
		}
	}

	protected void addItemToDelimitedLists(Set<String> keys, String itemToAdd, Table table) throws CbTestDbException {
		Set<String> failedKeys = new HashSet<String>();
		try {
			ArrayList<OperationFuture<Boolean>> futureOpsAppend = new ArrayList<OperationFuture<Boolean>>();
			ArrayList<OperationFuture<Boolean>> futureOpsAdd = new ArrayList<OperationFuture<Boolean>>();
			ArrayList<OperationFuture<Boolean>> futureOpsAppend2 = new ArrayList<OperationFuture<Boolean>>();
			
			// 1st try to append value
			for (String key : keys)
				futureOpsAppend.add(client[table.bucket.ordinal()].append(0, table.prefix + key, LIST_DELIMITER + itemToAdd));

			// if value doesn't exist, then add a new value
			for (OperationFuture<Boolean> op : futureOpsAppend)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue()) {
					futureOpsAdd.add(client[table.bucket.ordinal()].add(op.getKey(), 0, LIST_DELIMITER + itemToAdd));
				}
			
			// if value now exists (someone beat us to the add), then append again
			for (OperationFuture<Boolean> op : futureOpsAdd)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue()) {
					futureOpsAppend2.add(client[table.bucket.ordinal()].append(0, op.getKey(), LIST_DELIMITER + itemToAdd));
				}
			
			// make a list of all failed keys
			for (OperationFuture<Boolean> op : futureOpsAppend2)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue()) {
					failedKeys.add(op.getKey());
				}
			
			if (failedKeys.size() > 0)
				throw new CbTestDbException("Failed to append " + itemToAdd + " to the following keys: " + failedKeys.toString());
			
		} catch (Exception e) {
			throw new CbTestDbException("Error adding item " + itemToAdd + " to " + table.prefix + " list of " + keys + " - " + e.toString());
		}
	}

	protected void addItemsToDelimitedList(String key, Set<String> itemsToAdd, Table table)	throws CbTestDbException {
		String items = new String();
		for (String item : itemsToAdd)
			items += LIST_DELIMITER + item;
		try {
			// 1st try to append value
			boolean success = client[table.bucket.ordinal()].append(0, table.prefix + key, items).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
			if (!success) {
				// if value doesn't exist, then add a new value
				success = client[table.bucket.ordinal()].add(table.prefix + key, 0, items).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
				
				if (!success) {
					// if value now exists (someone beat us to the add), then append again
					success = client[table.bucket.ordinal()].append(0, table.prefix + key, items).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
					
					if (!success)
						throw new CbTestDbException("Tried appending, adding and appending again - but failed...");
				}
			}
		} catch (Exception e) {
			throw new CbTestDbException("Error adding items " + itemsToAdd.toString() + " to " + table.prefix + " list of " + key + " - " + e.toString());
		}
	}
	
	protected void addItemsToDelimitedLists(List<String> keys, List<String> itemsToAdd, Table table) throws CbTestDbException {
		if (keys.size() != itemsToAdd.size())
			throw new CbTestDbException("Invalid arguments, key list size must equal items list size");
		
		List<String> keysWithPrefix;
		if (table.prefix.isEmpty())
			keysWithPrefix = keys;
		else {
			keysWithPrefix = new ArrayList<String>();
			for (String key : keys)
				keysWithPrefix.add(table.prefix + key);
		}
		Set<String> failedKeys = new HashSet<String>();
		try {
			ArrayList<OperationFuture<Boolean>> futureOpsAppend = new ArrayList<OperationFuture<Boolean>>();
			ArrayList<OperationFuture<Boolean>> futureOpsAdd = new ArrayList<OperationFuture<Boolean>>();
			ArrayList<OperationFuture<Boolean>> futureOpsAppend2 = new ArrayList<OperationFuture<Boolean>>();
			
			// 1st try to append value
			for (int i = 0; i < keysWithPrefix.size(); i++)
				futureOpsAppend.add(client[table.bucket.ordinal()].append(0, keysWithPrefix.get(i), LIST_DELIMITER + itemsToAdd.get(i)));

			// if value doesn't exist, then add a new value
			for (OperationFuture<Boolean> op : futureOpsAppend)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue()) {
					futureOpsAdd.add(client[table.bucket.ordinal()].add(op.getKey(), 0, LIST_DELIMITER + itemsToAdd.get(keysWithPrefix.indexOf(op.getKey()))));
				}
			
			// if value now exists (someone beat us to the add), then append again
			for (OperationFuture<Boolean> op : futureOpsAdd)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue()) {
					futureOpsAppend2.add(client[table.bucket.ordinal()].append(0, op.getKey(), LIST_DELIMITER + itemsToAdd.get(keysWithPrefix.indexOf(op.getKey()))));
				}
			
			// make a list of all failed keys
			for (OperationFuture<Boolean> op : futureOpsAppend2)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue()) {
					failedKeys.add(op.getKey());
				}
			
			if (failedKeys.size() > 0)
				throw new CbTestDbException("Failed to append items to the following keys: " + failedKeys.toString());
			
		} catch (Exception e) {
			throw new CbTestDbException("Error adding items to " + table.prefix + " list of " + keys + " - " + e.toString());
		}
	}

	protected void removeItemFromDelimitedList(String key, String itemToRemove, Table table) throws CbTestDbException {
		try {
			client[table.bucket.ordinal()].append(0, table.prefix + key, LIST_DELIMITER + REMOVE_TOKEN + itemToRemove).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
		} catch (Exception e) {
			throw new CbTestDbException("Error removing item " + itemToRemove + " from " + table.prefix + " list of " + key + " - " + e.toString());
		}
	}

	protected void removeItemFromDelimitedLists(Set<String> keys, String itemToRemove, Table table) throws CbTestDbException {
		Set<String> failedKeys = new HashSet<String>();
		try {
			ArrayList<OperationFuture<Boolean>> futureOpsAppend = new ArrayList<OperationFuture<Boolean>>();
			
			for (String key : keys)
				futureOpsAppend.add(client[table.bucket.ordinal()].append(0, table.prefix + key, LIST_DELIMITER + REMOVE_TOKEN + itemToRemove));

			// make a list of all failed keys
			for (OperationFuture<Boolean> op : futureOpsAppend)
				if (!op.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue()) {
					failedKeys.add(op.getKey());
				}
			
			// doesn't matter if some of the keys are not found
//			if (failedKeys.size() > 0)
//				throw new CbTestDbException("Failed to append (remove) " + itemToRemove + " to the following keys: " + failedKeys.toString());
			
		} catch (Exception e) {
			throw new CbTestDbException("Error removing item " + itemToRemove + " from " + table.prefix + " list of " + keys + " - " + e.toString());
		}	
	}

	protected void removeAllItemsFromDelimitedList(String key, Table table) throws CbTestDbException {
		try {
			// it is ok to fail because no items were found
			client[table.bucket.ordinal()].delete(table.prefix + key).get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS).booleanValue();
		} catch (Exception e) {
			throw new CbTestDbException("Error removing all " + table.prefix + " list for " + key + " - " + e.toString());
		}
	}

	public Set<String> getAllItemsFromDelimitedList(String key, Table table) throws CbTestDbException {
		Set<String> items = new HashSet<String>();
		
		try {
			CASValue<Object> result = client[table.bucket.ordinal()].gets(table.prefix + key);
			if (result == null || result.getValue() == null)
				return items;

			// create list of items
			String[] itemList = ((String) result.getValue()).split(LIST_DELIMITER);
			for (String num : itemList) {
				if (num.length() > 0) {
					if (num.charAt(0) == REMOVE_TOKEN)
						items.remove(num.substring(1));
					else
						items.add(num);
				}
			}
			
			// if there are many removals, rewrite compressed list back to DB
			if (itemList.length - items.size() > MAX_LIST_REMOVALS_BEFORE_COMPRESS) {
				String newItemList = new String();
				for (String num : items)
					newItemList = LIST_DELIMITER + num;
				try {
					// only rewrite value if it didn't change
					client[table.bucket.ordinal()].cas(table.prefix + key, result.getCas(), newItemList);
					logger.debug("Compressed " + table.prefix + key + " from " + itemList.length + " items to " + items.size());
				} catch (Exception e) {
					// doesn't matter if this fails - maximum we will do it another time
				}
			}
			
			return items;
		} catch (Exception e) {
			throw new CbTestDbException("Error getting items from " + table.prefix + " list of " + key + " - " + e.toString());
		}
	}
	
	public Map<String,Set<String>> getAllItemsFromDelimitedLists(Set<String> keys, Table table) throws CbTestDbException {
		
		Map<String,Set<String>> listResults = new HashMap<String,Set<String>>();

		List<Future<CASValue<Object>>> futureCasOps = new ArrayList<Future<CASValue<Object>>>();
		List<String> casOpKeys = new ArrayList<String>();
		
		List<Future<CASResponse>> futureCasResponses = new ArrayList<Future<CASResponse>>();

		// async cas get all keys
		for (String key : keys) {
			futureCasOps.add(client[table.bucket.ordinal()].asyncGets(table.prefix + key));
			casOpKeys.add(key);
		}

		try {
			for (Future<CASValue<Object>> futureCasOp : futureCasOps) {
				CASValue<Object> cas = futureCasOp.get(CB_OP_TIMEOUT, TimeUnit.MILLISECONDS);
				if (cas == null)
					continue;
				String key = casOpKeys.get(futureCasOps.indexOf(futureCasOp));
				
				// create list of items
				Set<String> listResult = new HashSet<String>();
				String[] itemList = ((String) cas.getValue()).split(LIST_DELIMITER);
				for (String item : itemList) {
					if (item.length() > 0) {
						if (item.charAt(0) == REMOVE_TOKEN)
							listResult.remove(item.substring(1));
						else
							listResult.add(item);
					}
				}
				// add list to results
				listResults.put(key, listResult);
				
				// if there are many removals, rewrite compressed list back to DB
				if (itemList.length - listResult.size() > MAX_LIST_REMOVALS_BEFORE_COMPRESS) {
					String newItemList = new String();
					for (String item : listResult)
						newItemList = LIST_DELIMITER + item;
					// only rewrite value if it didn't change
					// doesn't matter if this fails - maximum we will do it another time
					futureCasResponses.add(client[table.bucket.ordinal()].asyncCAS(table.prefix + key, cas.getCas(), newItemList));
					logger.debug("Compressed " + table.prefix + key + " from " + itemList.length + " items to " + listResult.size());
				}
			}
		} catch (Exception e) {
			throw new CbTestDbException("Error getting items from " + table.prefix + " lists of " + keys + " - " + e.toString());
		}
		return listResults;
	}
	
	@Override
	protected void addPhone(String phone, String UDID) throws ViberDbItemExists, CbTestDbException {
		addItem(phone, UDID, 0, Table.Phones, true);
	}

	@Override
	protected void removePhone(String phone) throws ViberDbItemNotFound, CbTestDbException {
		removeItem(phone, Table.Phones, true);
	}

	@Override
	public String getUdidByPhone(String phone) throws ViberDbItemNotFound, CbTestDbException {
		return getItem(phone, Table.Phones, true);
	}
	
	@Override
	public Map<String, Object> getUdidsByPhones(Set<String> phones) throws CbTestDbException {
		return getItems(phones, Table.Phones, "", false);
	}

	@Override
	protected void addDeviceInfo(String UDID, DeviceInfo deviceInfo) throws ViberDbItemExists, CbTestDbException {
		addItem(UDID, gson.toJson(deviceInfo), 0, Table.Devices, true);
	}

	@Override
	protected void removeDeviceInfo(String UDID) throws ViberDbItemNotFound, CbTestDbException {
		removeItem(UDID, Table.Devices, true);
	}

	@Override
	public void updateDeviceInfo(String UDID, DeviceInfo deviceInfo) throws ViberDbItemNotFound, CbTestDbException {
		updateItem(UDID, gson.toJson(deviceInfo), Table.Devices, true);
	}

	@Override
	public DeviceInfo getDeviceInfo(String UDID) throws ViberDbItemNotFound, CbTestDbException {
		return gson.fromJson(getItem(UDID, Table.Devices, true), DeviceInfo.class);
	}

	@Override
	protected void addAddressBook(String UDID, AddressBook addressBook)	throws ViberDbItemExists, CbTestDbException {
		List<String> keys = new ArrayList<String>();
		List<String> contacts = new ArrayList<String>();
		for (int i = 0; i < abtype.size; i++) {
			keys.add(UDID + "_" + i);
			contacts.add("");
		}
		for (Entry<String, AbContact> contactEntry : addressBook.getContacts().entrySet()) {
			String phone = contactEntry.getKey();
			try {
				Long phoneNum = new Long(phone);
				String contact = phone + AB_CONTACT_DELIMITER + contactEntry.getValue().name + AB_CONTACT_DELIMITER + contactEntry.getValue().orgNum;
				contacts.set(((int) (phoneNum % abtype.size)),contacts.get((int) (phoneNum % abtype.size)).concat(LIST_DELIMITER + contact));
			} catch (NumberFormatException e) {
				logger.warn("Invalid phone number " + phone + " in address book for UDID " + UDID);
			}
		}
		addItemsToDelimitedLists(keys, contacts, Table.AB);
	}

	@Override
	protected void removeAddressBook(String UDID) throws ViberDbItemNotFound, CbTestDbException {
		Set<String> keys = new HashSet<String>();
		for (int i = 0; i < abtype.size; i++)
			keys.add(UDID + "_" + i);
		
		removeItems(keys, Table.AB, "", true);
	}
	
	@Override
	public void updateAddressBook(String UDID, AddressBook addedEntries, AddressBook removedEntries) 
			throws ViberDbItemNotFound,	CbTestDbException {

		List<String> keys = new ArrayList<String>();
		List<String> contacts = new ArrayList<String>();
		for (int i = 0; i < abtype.size; i++) {
			keys.add(UDID + "_" + i);
			contacts.add("");
		}
		for (Entry<String, AbContact> contactEntry : removedEntries.getContacts().entrySet()) {
			String phone = contactEntry.getKey();
			try {
				Long phoneNum = new Long(phone);
				int index = (int) (phoneNum % abtype.size);
				String contact = phone + AB_CONTACT_DELIMITER + contactEntry.getValue().name + AB_CONTACT_DELIMITER + contactEntry.getValue().orgNum;				
				contacts.set(index,contacts.get(index).concat(LIST_DELIMITER + REMOVE_TOKEN + contact));
			} catch (NumberFormatException e) {
				logger.warn("Invalid phone number " + phone + " in address book for UDID " + UDID);
			}
		}
		for (Entry<String, AbContact> contactEntry : addedEntries.getContacts().entrySet()) {
			String phone = contactEntry.getKey();
			try {
				Long phoneNum = new Long(phone);
				int index = (int) (phoneNum % abtype.size);
				String contact = phone + AB_CONTACT_DELIMITER + contactEntry.getValue().name + AB_CONTACT_DELIMITER + contactEntry.getValue().orgNum;				
				contacts.set(index,contacts.get(index).concat(LIST_DELIMITER + contact));
			} catch (NumberFormatException e) {
				logger.warn("Invalid phone number " + phone + " in address book for UDID " + UDID);
			}
		}
		// only update keys that have values
		for (int i=contacts.size()-1; i >= 0; i--) {
			if (contacts.get(i).isEmpty()) {
				keys.remove(i);
				contacts.remove(i);
			}
		}
		addItemsToDelimitedLists(keys, contacts, Table.AB);
	}

	@Override
	public AddressBook getAddressBook(String UDID) throws ViberDbItemNotFound, CbTestDbException {
		Set<String> uuids = new HashSet<String>();
		for (int k = 0; k < abtype.size; k++)
			uuids.add(UDID + "_" + k);
		
		Map<String, Set<String>> results = getAllItemsFromDelimitedLists(uuids, Table.AB);
		
		if (results.size() == 0)
			throw new ViberDbItemNotFound("Address book for " + UDID + " not found");
		
		AddressBook fullAb = new AddressBook();
		for (Set<String> itemList : results.values()) {
			for (String item : itemList) {
				String[] itemParts = item.split(AB_CONTACT_DELIMITER);
				if (itemParts.length == 3) 
					fullAb.addContact(itemParts[0], new AbContact(itemParts[1], itemParts[2]));
			}
		}
		return fullAb;
	}

	@Override
	public AbContact getContact(String UDID, String phoneNum) throws ViberDbItemNotFound, CbTestDbException {
		Long lPhoneNum;
		try {
			lPhoneNum = new Long(phoneNum);
		} catch (NumberFormatException e) {
			throw new ViberDbItemNotFound("getContact: Invalid phone number " + phoneNum + " for UDID " + UDID);
		}
		
		Set<String> itemList = getAllItemsFromDelimitedList(UDID + "_" + (lPhoneNum % abtype.size), Table.AB);
		
		for (String item : itemList) {
			if (item.startsWith(phoneNum)) {
				String[] itemParts = item.split(AB_CONTACT_DELIMITER);
				if (itemParts.length == 3) 
					return new AbContact(itemParts[1], itemParts[2]);
				else
					throw new CbTestDbException("Error getting contact for phone " + phoneNum + ", invalid format: " + item);
			}
		}
		throw new ViberDbItemNotFound("Could not find contact " + phoneNum + " in phone book of " + UDID);
	}

	@Override
	public Contact getRandomContact(String UDID, int retries) throws ViberDbItemNotFound, CbTestDbException {
		Set<String> itemList = null;
		for (int i = 0; i < retries; i++) {
			itemList = getAllItemsFromDelimitedList(UDID + "_" + random.nextInt(abtype.size), Table.AB);
			if (itemList.size() > 0)
				break;
		}
		if (itemList.size() == 0)
			throw new ViberDbItemNotFound("Could not find random contact in phone book of " + UDID);
		
		int itemNum = random.nextInt(itemList.size());
		String item = (String) itemList.toArray()[itemNum];
		String[] itemParts = item.split(AB_CONTACT_DELIMITER);
		if (itemParts.length == 3) 
			return new Contact(itemParts[0], new AbContact(itemParts[1], itemParts[2]));
		else
			throw new CbTestDbException("Error getting random contact for phone, invalid format: " + item);
	}
	
	@Override
	protected void addNumberToRab(String phone, String numToAdd) throws CbTestDbException {
		addItemToDelimitedList(phone, numToAdd, Table.RevAB);
	}

	@Override
	protected void addNumberToRab(Set<String> phones, String numToAdd) throws CbTestDbException {
		addItemToDelimitedLists(phones, numToAdd, Table.RevAB);
	}

	@Override
	protected void removeNumberFromRab(String phone, String numToRemove) throws CbTestDbException {
		removeItemFromDelimitedList(phone, numToRemove, Table.RevAB);
	}

	@Override
	protected void removeNumberFromRab(Set<String> phones, String numToRemove) throws CbTestDbException {
		removeItemFromDelimitedLists(phones, numToRemove, Table.RevAB);
	}

	@Override
	public Set<String> getNumbersFromRab(String phone) throws CbTestDbException {
		return getAllItemsFromDelimitedList(phone, Table.RevAB);
	}

	@Override
	protected void addNumberToRegNums(String phone, String numToAdd) throws CbTestDbException {
		addItemToDelimitedList(phone, numToAdd, Table.RegNums);		
	}

	@Override
	protected void addNumberToRegNums(Set<String> phones, String numToAdd) throws CbTestDbException {
		addItemToDelimitedLists(phones, numToAdd, Table.RegNums);		
	}

	@Override
	protected void addNumbersToRegNums(String phone, Set<String> numsToAdd)	throws CbTestDbException {
		addItemsToDelimitedList(phone, numsToAdd, Table.RegNums);
	}

	@Override
	protected void removeNumberFromRegNums(String phone, String numToRemove) throws CbTestDbException {
		removeItemFromDelimitedList(phone, numToRemove, Table.RegNums);
	}

	@Override
	protected void removeNumberFromRegNums(Set<String> phones, String numToRemove) throws CbTestDbException {
		removeItemFromDelimitedLists(phones, numToRemove, Table.RegNums);
	}

	@Override
	protected void removeAllRegNums(String phone) throws CbTestDbException {
		removeAllItemsFromDelimitedList(phone, Table.RegNums);
	}

	@Override
	public Set<String> getNumbersFromRegNums(String phone) throws CbTestDbException {
		return getAllItemsFromDelimitedList(phone, Table.RegNums);
	}	

	@Override
	public void addMessage(MessageType messageType, String UDID, MessageInfo messageInfo, int expiryDays) throws CbTestDbException {
		// add message token to list
		addItemToDelimitedList(UDID, messageInfo.msgToken, 
				messageType.equals(MessageType.SentMsg) ? Table.MsgsIdx : Table.DelMsgsIdx);
		
		// add message info to separate key
		addItem(UDID + "_" + messageInfo.msgToken, gson.toJson(messageInfo), expiryDays*24*60*60, 
				messageType.equals(MessageType.SentMsg) ? Table.Msgs : Table.DelMsgs, false);
	}

	@Override
	public ArrayList<MessageInfo> getAllMessages(MessageType messageType, String UDID) throws CbTestDbException {
		Set<String> msgTokens = getAllItemsFromDelimitedList(UDID, 
				messageType.equals(MessageType.SentMsg) ? Table.MsgsIdx : Table.DelMsgsIdx);
		
		Map<String, Object> msgs = getItems(msgTokens, messageType.equals(MessageType.SentMsg) ? Table.Msgs : Table.DelMsgs, UDID + "_", false);
		
		ArrayList<MessageInfo> msgInfos = new ArrayList<MessageInfo>();
		for (Object msg : msgs.values())
			msgInfos.add(gson.fromJson((String) msg, MessageInfo.class));
		return msgInfos;
	}

	@Override
	public void removeMessageByMsgToken(MessageType messageType, String UDID, String msgToken) throws ViberDbItemNotFound, CbTestDbException {
		removeItem(UDID + "_" + msgToken, messageType.equals(MessageType.SentMsg) ? Table.Msgs : Table.DelMsgs, false);
		removeItemFromDelimitedList(UDID, msgToken, messageType.equals(MessageType.SentMsg) ? Table.MsgsIdx : Table.DelMsgsIdx);
	}

	@Override
	public void removeAllMessages(MessageType messageType, String UDID) throws CbTestDbException {
		Set<String> msgTokens = getAllItemsFromDelimitedList(UDID, messageType.equals(MessageType.SentMsg) ? Table.MsgsIdx : Table.DelMsgsIdx);
		removeAllItemsFromDelimitedList(UDID, messageType.equals(MessageType.SentMsg) ? Table.MsgsIdx : Table.DelMsgsIdx);
		removeItems(msgTokens, messageType.equals(MessageType.SentMsg) ? Table.Msgs : Table.DelMsgs, UDID + "_", false);
	}

	@Override
	public void addCall(MessageType messageType, String UDID, CallInfo callInfo, int expiryDays)	throws CbTestDbException {
		// add call token to list
		addItemToDelimitedList(UDID, callInfo.callToken, messageType.equals(MessageType.SentMsg) ? Table.CallsIdx : Table.DelCallsIdx);
		
		// add call info to separate key
		addItem(UDID + "_" + callInfo.callToken, gson.toJson(callInfo), expiryDays*24*60*60, 
				messageType.equals(MessageType.SentMsg) ? Table.Calls : Table.DelCalls, false);
	}

	@Override
	public ArrayList<CallInfo> getAllCalls(MessageType messageType, String UDID) throws CbTestDbException {
		Set<String> callTokens = getAllItemsFromDelimitedList(UDID, 
				messageType.equals(MessageType.SentMsg) ? Table.CallsIdx : Table.DelCallsIdx);
		
		Map<String, Object> msgs = getItems(callTokens, messageType.equals(MessageType.SentMsg) ? Table.Calls : Table.DelCalls, UDID + "_", false);
		
		ArrayList<CallInfo> callInfos = new ArrayList<CallInfo>();
		for (Object msg : msgs.values())
			callInfos.add(gson.fromJson((String) msg, CallInfo.class));
		return callInfos;
	}

	@Override
	public void removeCallByCallToken(MessageType messageType, String UDID, String callToken) throws ViberDbItemNotFound, CbTestDbException {
		removeItem(UDID + "_" + callToken, messageType.equals(MessageType.SentMsg) ? Table.Calls : Table.DelCalls, false);
		removeItemFromDelimitedList(UDID, callToken, messageType.equals(MessageType.SentMsg) ? Table.CallsIdx : Table.DelCallsIdx);
	}

	@Override
	public void removeAllCalls(MessageType messageType, String UDID) throws CbTestDbException {
		Set<String> callTokens = getAllItemsFromDelimitedList(UDID, messageType.equals(MessageType.SentMsg) ? Table.CallsIdx : Table.DelCallsIdx);
		removeAllItemsFromDelimitedList(UDID, messageType.equals(MessageType.SentMsg) ? Table.CallsIdx : Table.DelCallsIdx);
		removeItems(callTokens, messageType.equals(MessageType.SentMsg) ? Table.Calls : Table.DelCalls, UDID + "_", false);
	}
}


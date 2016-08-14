package com.cbtest.largescaletest;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.cbtest.common.RandomString;
import com.cbtest.largescaletest.iteminfo.AbContact;
import com.cbtest.largescaletest.iteminfo.AddressBook;
import com.cbtest.largescaletest.iteminfo.CallInfo;
import com.cbtest.largescaletest.iteminfo.Contact;
import com.cbtest.largescaletest.iteminfo.DeviceInfo;
import com.cbtest.largescaletest.iteminfo.MessageInfo;

public abstract class DbMediator {
	private static final int UDID_LEN = 40;
	private static final int MSG_EXPIRY_DAYS = 14;
	private static final int CALL_EXPIRY_DAYS = 1;
	protected static Logger logger = Logger.getLogger(DbMediator.class);
	
	public enum MessageType {
		SentMsg,
		DeliveredMsg,
	}

	public abstract void connectToDb() throws CbTestDbException;
	public abstract void disconnectDb();
	public abstract long getNumRecords() throws CbTestDbException;

	// Phone
	protected abstract void addPhone(String phone, String UDID) throws ViberDbItemExists, CbTestDbException;
	protected abstract void removePhone(String phone) throws ViberDbItemNotFound, CbTestDbException;
	public abstract String getUdidByPhone(String phone) throws ViberDbItemNotFound, CbTestDbException;
	public abstract Map<String, Object> getUdidsByPhones(Set<String> phones) throws CbTestDbException;
	
	// Device Info
	protected abstract void addDeviceInfo(String UDID, DeviceInfo deviceInfo) throws ViberDbItemExists, CbTestDbException;
	protected abstract void removeDeviceInfo(String UDID) throws ViberDbItemNotFound, CbTestDbException;
	public abstract void updateDeviceInfo(String UDID, DeviceInfo deviceInfo) throws ViberDbItemNotFound, CbTestDbException;
	public abstract DeviceInfo getDeviceInfo(String UDID) throws ViberDbItemNotFound, CbTestDbException;
	
	// Address Book
	protected abstract void addAddressBook(String UDID, AddressBook addressBook) throws ViberDbItemExists, CbTestDbException;
	protected abstract void removeAddressBook(String UDID) throws ViberDbItemNotFound, CbTestDbException;
	public abstract void updateAddressBook(String UDID, AddressBook addedEntries, AddressBook removedEntries) throws ViberDbItemNotFound, ViberDbItemModified, CbTestDbException;
	public abstract AddressBook getAddressBook(String UDID) throws ViberDbItemNotFound, CbTestDbException;
	public abstract AbContact getContact(String UDID, String phoneNum) throws ViberDbItemNotFound, CbTestDbException;
	public abstract Contact getRandomContact(String UDID, int retries) throws ViberDbItemNotFound, CbTestDbException;

	// Reverse Address Book (RAB)
	protected abstract void addNumberToRab(String phone, String numToAdd) throws CbTestDbException;
	protected abstract void addNumberToRab(Set<String> phones, String numToAdd) throws CbTestDbException;
	protected abstract void removeNumberFromRab(String phone, String numToRemove) throws CbTestDbException;
	protected abstract void removeNumberFromRab(Set<String> phones, String numToRemove) throws CbTestDbException;
	public abstract Set<String> getNumbersFromRab(String phone) throws CbTestDbException;
	
	// Registered Numbers
	protected abstract void addNumberToRegNums(String phone, String numToAdd) throws CbTestDbException;
	protected abstract void addNumberToRegNums(Set<String> phones, String numToAdd) throws CbTestDbException;
	protected abstract void addNumbersToRegNums(String phone, Set<String> numsToAdd) throws CbTestDbException;
	protected abstract void removeNumberFromRegNums(String phone, String numToRemove) throws CbTestDbException;
	protected abstract void removeNumberFromRegNums(Set<String> phones, String numToRemove) throws CbTestDbException;
	protected abstract void removeAllRegNums(String phone) throws CbTestDbException;
	public abstract Set<String> getNumbersFromRegNums(String phone) throws CbTestDbException;
	
	// Messages
	public abstract void addMessage(MessageType msgType, String UDID, MessageInfo messageInfo, int expiryDays) throws CbTestDbException;
	public abstract ArrayList<MessageInfo> getAllMessages(MessageType msgType, String UDID) throws CbTestDbException;
	public abstract void removeMessageByMsgToken(MessageType msgType, String UDID, String msgToken) throws ViberDbItemNotFound, CbTestDbException;
	public abstract void removeAllMessages(MessageType msgType, String UDID) throws CbTestDbException;
	
	// Calls
	public abstract void addCall(MessageType msgType, String UDID, CallInfo callInfo, int expiryDays) throws CbTestDbException;
	public abstract ArrayList<CallInfo> getAllCalls(MessageType msgType, String UDID) throws CbTestDbException;
	public abstract void removeCallByCallToken(MessageType msgType, String UDID, String callToken) throws ViberDbItemNotFound, CbTestDbException;
	public abstract void removeAllCalls(MessageType msgType, String UDID) throws CbTestDbException;
	
	// Logical Operations
	public void addUser(String phone, String UDID, DeviceInfo deviceInfo, AddressBook addressBook) throws ViberDbItemExists, CbTestDbException {
		try {
			addPhone(phone, UDID);
			
			addDeviceInfo(UDID, deviceInfo);
	
			addAddressBook(UDID, addressBook);
			
			// add users number to RAB of each contact
			addNumberToRab(addressBook.getContacts().keySet(), phone);
			
			// get users RAB and add users phone to the regNums of each user
			Set<String> rab = getNumbersFromRab(phone);
			addNumberToRegNums(rab, phone);
	
			// go over all contacts, get all registered users and add to users registered numbers
			Map<String, Object> regNums = getUdidsByPhones(addressBook.getContacts().keySet());
			addNumbersToRegNums(phone, regNums.keySet());
		} catch (ViberDbItemExists e) {
			throw e;			
		} catch (CbTestDbException e) {
			// if add user fails, remove user and throw exception that occurred
			try {
				logger.warn("Couldn't add user " + phone + " (recalling user) - " + e);
				removeUser(phone);
			} catch (CbTestDbException e1) {}
			throw e;
		}
	}

	public void removeUser(String phone) throws ViberDbItemNotFound, CbTestDbException {
		String UDID = getUdidByPhone(phone);

		// catch exceptions for each operation
		// and perform all operations even if one fails
		// return most severe exception if occurs
		CbTestDbException dbExecption = null;
		
		// remove all users's messages and calls
		try {
			removeAllMessages(MessageType.SentMsg, UDID);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		try {
			removeAllMessages(MessageType.DeliveredMsg, UDID);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		try {
			removeAllCalls(MessageType.SentMsg, UDID);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		try {
			removeAllCalls(MessageType.DeliveredMsg, UDID);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		
		// remove user from regNums of all other users
		try {		
			Set<String> regNums = getNumbersFromRegNums(phone);
			removeNumberFromRegNums(regNums, phone);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		
		// delete all regNums
		try {	
			removeAllRegNums(phone);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		
		// get address book and remove users phone from rab of all contacts
		try {
			AddressBook addressBook = getAddressBook(UDID);
			removeNumberFromRab(addressBook.getContacts().keySet(), phone);				
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		
		try {		
			removeAddressBook(UDID);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		try {
			removeDeviceInfo(UDID);
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		try {
			removePhone(phone);		
		} catch (CbTestDbException e) {
			if (dbExecption == null || dbExecption instanceof ViberDbItemNotFound)
				dbExecption = e;
		}
		
		if (dbExecption != null)
			throw dbExecption;
	}
	
	public void sendMessage(String phone, MessageInfo messageInfo, boolean removeMessage) throws ViberDbItemNotFound, CbTestDbException {
		String targetUDID = getUdidByPhone(phone);
		String srcUDID = getUdidByPhone(messageInfo.srcPhone);
		
		// get device info
		@SuppressWarnings("unused")
		DeviceInfo deviceInfo = getDeviceInfo(targetUDID);
		
		// get source user's name / phone as appears in target phone's address book
		try {
			@SuppressWarnings("unused")
			AbContact abContact = getContact(targetUDID, messageInfo.srcPhone);
		} catch (ViberDbItemNotFound e) {
		}
		
		// add message to target phone's msg list 
		addMessage(MessageType.SentMsg, targetUDID, messageInfo, MSG_EXPIRY_DAYS);
		
		// target has received message and sent ack
		if (removeMessage)
			removeMessageByMsgToken(MessageType.SentMsg, targetUDID, messageInfo.msgToken);
		
		// add message to source phone's delivered list
		messageInfo.text += " - delivered";
		addMessage(MessageType.DeliveredMsg, srcUDID, messageInfo, 0);
		
		// source phone has received delivery and sent ack		
		if (removeMessage)
			removeMessageByMsgToken(MessageType.DeliveredMsg, srcUDID, messageInfo.msgToken);
	}
	
	public void callPhone(String phone, CallInfo callInfo, boolean removeCall) throws ViberDbItemNotFound, CbTestDbException {
		String targetUDID = getUdidByPhone(phone);
		
		// get device info
		@SuppressWarnings("unused")
		DeviceInfo deviceInfo = getDeviceInfo(targetUDID);
		
		// get source user's name / phone as appears in target phone's address book
		try {
			if (callInfo.srcPhone.isEmpty()) {
				Contact contact;
				contact = getRandomContact(targetUDID, 3);
				callInfo.srcPhone = contact.phoneNum;
				callInfo.contactName = contact.abContact.name;
			}
			else
				callInfo.contactName = getContact(targetUDID, callInfo.srcPhone).name;
		} catch (ViberDbItemNotFound e) {
		}
		String srcUDID = getUdidByPhone(callInfo.srcPhone);
		
		// add call to target phone's call list 
		addCall(MessageType.SentMsg, targetUDID, callInfo, CALL_EXPIRY_DAYS);
		
		// target has received call and sent ack
		if (removeCall)
			removeCallByCallToken(MessageType.SentMsg, targetUDID, callInfo.callToken);
		
		// add call to source phone's delivered list
		addCall(MessageType.DeliveredMsg, srcUDID, callInfo, 0);
		
		// source phone has received delivery and sent ack
		if (removeCall)
			removeCallByCallToken(MessageType.DeliveredMsg, srcUDID, callInfo.callToken);
	}

	public void login(String phone) throws ViberDbItemNotFound, CbTestDbException {
		String UDID = getUdidByPhone(phone);
	
		// get device info
		@SuppressWarnings("unused")
		DeviceInfo deviceInfo = getDeviceInfo(UDID);
		
		// get recent messages and remove them one by one
		ArrayList<MessageInfo> msgs = getAllMessages(MessageType.SentMsg, UDID);
		for (MessageInfo msgInfo : msgs) 
			removeMessageByMsgToken(MessageType.SentMsg, UDID, msgInfo.msgToken);

		// get recent delivered messages and remove them one by one
		msgs = getAllMessages(MessageType.DeliveredMsg, UDID);
		for (MessageInfo msgInfo : msgs) 
			removeMessageByMsgToken(MessageType.DeliveredMsg, UDID, msgInfo.msgToken);
		
		// get recent calls and remove them one by one
		ArrayList<CallInfo> calls = getAllCalls(MessageType.SentMsg, UDID);
		for (CallInfo callInfo : calls)
			removeCallByCallToken(MessageType.SentMsg, UDID, callInfo.callToken);

		// get recent delivered calls and remove them one by one
		calls = getAllCalls(MessageType.DeliveredMsg, UDID);
		for (CallInfo callInfo : calls)
			removeCallByCallToken(MessageType.DeliveredMsg, UDID, callInfo.callToken);
	}
	
	public void shareAddressBook(String phone, String UDID, AddressBook addedEntries, AddressBook removedEntries) throws ViberDbItemNotFound, ViberDbItemModified, CbTestDbException {
		// update address book with entries to add and remove
		try {
			updateAddressBook(UDID, addedEntries, removedEntries);
		} catch (ViberDbItemModified e) {
			logger.info("Address book was modified during update, trying again - " + e);
			// if the item was modified during update, try again
			updateAddressBook(UDID, addedEntries, removedEntries);
		}

		// remove users number to RAB of each removed contact
		for (String contactPhone : removedEntries.getContacts().keySet()) { 
			removeNumberFromRab(contactPhone, phone);				
		}

		// add users number to RAB of each added contact
		addNumberToRab(addedEntries.getContacts().keySet(), phone);
	}

	public void shareAddressBook(String phone, AddressBook addedEntries, int numContactsToRemove, long maxPhoneToRemove) throws ViberDbItemNotFound, CbTestDbException {
		String UDID = getUdidByPhone(phone);
		
		// set contacts to remove
		HashMap<String,AbContact> contactsToRemove = new HashMap<String,AbContact>();
		if (numContactsToRemove > 0) {
			AddressBook addressBook = getAddressBook(UDID);
			for (Entry<String, AbContact> contact : addressBook.getContacts().entrySet()) {
				if (maxPhoneToRemove <= 0 || new Long(contact.getKey()) <= maxPhoneToRemove) {
					contactsToRemove.put(contact.getKey(), contact.getValue());
					if (contactsToRemove.size() == numContactsToRemove)
						break;
				}
			}
		}
		
		if (contactsToRemove.size() != numContactsToRemove)
			logger.warn("Couldn't find " + numContactsToRemove + " contact(s) to remove from AB of " + phone);
		
		shareAddressBook(phone, UDID, addedEntries, new AddressBook(contactsToRemove));
	}

	public void userIsTyping(String phone) throws ViberDbItemNotFound, CbTestDbException {
		String UDID = getUdidByPhone(phone);
		
		// get device info
		@SuppressWarnings("unused")
		DeviceInfo deviceInfo = getDeviceInfo(UDID);
	}
	
	public ValidationInfo validateUser(String phone, String UDID) {
		StringBuilder validInfo = new StringBuilder("Validation info for phone " + phone);
		boolean isValid = true;
		try {
			UDID = getUdidByPhone(phone);			
			validInfo.append("\nUDID = " + UDID);
		} catch (CbTestDbException e) {
			isValid = false;
			validInfo.append("\ngetUdidByPhone failed = " + e);
		}
		if (UDID == null || UDID.isEmpty()) {
			logger.debug(validInfo);
			return new ValidationInfo(isValid, validInfo.toString());
		}
		if (UDID.length() != UDID_LEN) { 
			isValid = false;
			validInfo.append(", UDID length is invalid = " + UDID.length());
		}
		
		int last2digits = new Integer(phone.substring(phone.length() - 2));
		try {
			// get & validate device info
			DeviceInfo deviceInfo = getDeviceInfo(UDID);
			validInfo.append("\ndeviceInfo = " + deviceInfo);
			if (deviceInfo.system != last2digits) {
				isValid = false;
				validInfo.append(", system is invalid - should be " + last2digits);
			}
		} catch (CbTestDbException e) {
			isValid = false;
			validInfo.append("\ngetDeviceInfo failed = " + e);
		}
		
		try {
			// get & validate address book
			AddressBook addressBook = getAddressBook(UDID);
			validInfo.append("\naddressBook = " + addressBook);
			int numAbEntries = 5 * (last2digits + 1);
			// check that there is maximum 10% entries more or less then the original number (there is a chance it will be more if 2 share AB are active at once)
			if (Math.abs(addressBook.getContacts().size()-numAbEntries) > (numAbEntries / 10)) {
//				isValid = false;
//				validInfo.append(", invalid number of AB entries. Currently there are " + addressBook.getContacts().size() + " enties, should be " + numAbEntries + " entries");
			}
		} catch (CbTestDbException e) {
			isValid = false;
			validInfo.append("\ngetAddressBook failed = " + e);
		}
		
		try {
			// get & validate RAB
			Set<String> rab = getNumbersFromRab(phone);
			// TODO: validate RAB
			validInfo.append("\nRAB = " + rab.toString());
			
			// get & validate Registered Numbers
			Set<String> regNums = getNumbersFromRegNums(phone);
			// TODO: validate Registered Numbers
			validInfo.append("\nRegNums = " + regNums.toString());
			
			ArrayList<MessageInfo> msgs = getAllMessages(MessageType.SentMsg, UDID);
			validInfo.append("\nmsgs = " + msgs.toString());
			
			msgs = getAllMessages(MessageType.DeliveredMsg, UDID);
			validInfo.append("\ndelivered msgs = " + msgs.toString());
			
			ArrayList<CallInfo> calls = getAllCalls(MessageType.SentMsg, UDID);
			validInfo.append("\ncalls = " + calls.toString());
			
			calls = getAllCalls(MessageType.DeliveredMsg, UDID);
			validInfo.append("\ndelivered calls = " + calls.toString());
			
			return new ValidationInfo(isValid, validInfo.toString());
		} catch (CbTestDbException e) {
			validInfo.append("\n\nException occurred = " + e);
			return new ValidationInfo(false, validInfo.toString());
		} finally {
			logger.debug(new ValidationInfo(false, validInfo.toString()));
		}
	}
	
	public static void test1(DbMediator db) {
		String phone = "972507336652"; 
		String UDID = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		try {
			// Phones
			db.addPhone(phone, UDID);
			System.out.println(db.getUdidByPhone(phone));
			
			// Device info
			db.addDeviceInfo(UDID, new DeviceInfo(phone, "capabilities", "deviceKey", "pushToken", "version", 1, 2, 3));
			System.out.println(db.getDeviceInfo(UDID));
			
			db.userIsTyping(phone);
			
			// Address book
			AddressBook addressBook = new AddressBook();
			long basePhoneNum = 100000000L;
			for (int i = 0; i < 25; i++) {
				long phoneNum = basePhoneNum + i;
				addressBook.addContact("111" + phoneNum, new AbContact(RandomString.getName(25), new Long(phoneNum).toString()));
			}
			db.addAddressBook(UDID, addressBook);
			AddressBook abFromDb = db.getAddressBook(UDID);
			System.out.println("Ab size = " + abFromDb.getContacts().size() + ", ab = " + abFromDb);			
			System.out.println(db.getContact(UDID, "111" + (basePhoneNum + 8)));
			
			AddressBook addedEntries = new AddressBook();
			AddressBook removedEntries = new AddressBook();
			long phoneNum = 100000000L + 25;
			addedEntries.addContact("111" + phoneNum, new AbContact(RandomString.getName(25), new Long(phoneNum).toString()));
			phoneNum = 100000000L + 0;
			removedEntries.addContact("111" + phoneNum, new AbContact(RandomString.getName(25), new Long(phoneNum).toString()));
			db.updateAddressBook(UDID, addedEntries, removedEntries);
			
			// RAB
			db.removeNumberFromRab(phone, "111" + (basePhoneNum + 1));
			for (int i = 0; i < 5; i++)
				db.addNumberToRab(phone, "111" + (basePhoneNum + i));
			System.out.println("RAB = " + db.getNumbersFromRab(phone));
			
			for (int i = 0; i < 3; i++)
				db.removeNumberFromRab(phone, "111" + (basePhoneNum + i));
			System.out.println("RAB = " + db.getNumbersFromRab(phone));
			
			for (int i = 3; i < 20; i++)
				db.removeNumberFromRab(phone, "111" + (basePhoneNum + i));
			System.out.println("RAB = " + db.getNumbersFromRab(phone));

			// Registered Numbers
			db.removeNumberFromRegNums(phone, "111" + (basePhoneNum + 1));
			for (int i = 0; i < 5; i++)
				db.addNumberToRegNums(phone, "111" + (basePhoneNum + i));
			System.out.println("RegNums = " + db.getNumbersFromRegNums(phone));
			
			for (int i = 0; i < 3; i++)
				db.removeNumberFromRegNums(phone, "111" + (basePhoneNum + i));
			System.out.println("RegNums = " + db.getNumbersFromRegNums(phone));
			
			for (int i = 3; i < 20; i++)
				db.removeNumberFromRegNums(phone, "111" + (basePhoneNum + i));
			System.out.println("RegNums = " + db.getNumbersFromRegNums(phone));
			
			Set<String> numsToAdd = new HashSet<String>();
			for (int i = 0; i < 5; i++)
				numsToAdd.add( "111" + (basePhoneNum + i));
			db.addNumbersToRegNums(phone, numsToAdd);
			System.out.println("RegNums = " + db.getNumbersFromRegNums(phone));
			
			db.removeAllRegNums(phone);
			System.out.println("RegNums = " + db.getNumbersFromRegNums(phone));

			// Messages
			long baseMsgToken = 100000000L;
			for (MessageType msgType : MessageType.values()) {
				System.out.println("MsgType = " + msgType);
				for (int i = 1; i <= 5; i++)
					db.addMessage(msgType, UDID, new MessageInfo(""+(baseMsgToken+i), ""+(phoneNum+i), new Date(), "Test message #" + i), MSG_EXPIRY_DAYS);
				System.out.println(db.getAllMessages(msgType, UDID).toString());
				
				for (int i = 1; i <= 3; i++)
					db.removeMessageByMsgToken(msgType, UDID, ""+(baseMsgToken+i));
				System.out.println(db.getAllMessages(msgType, UDID).toString());
				
				db.removeAllMessages(msgType, UDID);
				System.out.println(db.getAllMessages(msgType, UDID).toString());
				
				// Calls
				long baseCallToken = 100000000L;
				for (int i = 1; i <= 5; i++)
					db.addCall(msgType, UDID, new CallInfo(""+(baseCallToken+i), ""+(phoneNum+i), new Date()), CALL_EXPIRY_DAYS);
				System.out.println(db.getAllCalls(msgType, UDID).toString());
				
				for (int i = 1; i <= 3; i++)
					db.removeCallByCallToken(msgType, UDID, ""+(baseCallToken+i));
				System.out.println(db.getAllCalls(msgType, UDID).toString());
				
				db.removeAllCalls(msgType, UDID);
				System.out.println(db.getAllCalls(msgType, UDID).toString());
			}
			
			db.validateUser(phone, UDID);
		} catch (CbTestDbException e) {
			e.printStackTrace();
		} finally {
			try {
				db.removeAddressBook(UDID);
				db.removeDeviceInfo(UDID);
				db.removePhone(phone);
			} catch (CbTestDbException e) {}
		}
	}
	
	public static void test2(DbMediator db) {
		String phone1 = "100000000"; 
		String phone2 = "200000000"; 
		String phone3 = "300000000"; 
		String phone4 = "400000000"; 
		String phone5 = "500000000"; 
		String UDID1 = "AAAAA";
		String UDID2 = "BBBBB";
		String UDID3 = "CCCCC";
		String UDID4 = "DDDDD";
		String UDID5 = "EEEEE";
		long baseMsgToken = 100000000L;
		long baseCallToken = 100000000L;
		try {
			// create phone1
			DeviceInfo deviceInfo = new DeviceInfo(phone1, "capabilities", "deviceKey", "pushToken", "version", 1, 2, 3);
			AddressBook addressBook = new AddressBook();
			addressBook.addContact(phone2, new AbContact("phone2", phone2));
			db.addUser(phone1, UDID1, deviceInfo, addressBook);

			System.out.println("\n> After creating phone1");
			db.validateUser(phone1, UDID1);
			db.validateUser(phone2, UDID2);

			// create phone2
			DeviceInfo deviceInfo2 = new DeviceInfo(phone2, "capabilities", "deviceKey", "pushToken", "version", 1, 2, 3);
			AddressBook addressBook2 = new AddressBook();
			addressBook2.addContact(phone1, new AbContact("phone1", phone1));
			db.addUser(phone2, UDID2, deviceInfo2, addressBook2);
			
			System.out.println("\n> After creating phone2");
			db.validateUser(phone1, UDID1);
			db.validateUser(phone2, UDID2);

			// send msgs from phone2 to phone1
			for (int i = 1; i <= 5; i++)
				db.sendMessage(phone1, new MessageInfo(""+(++baseMsgToken), phone2, new Date(), "Msg #" + i + " from " + phone2 + " to " + phone1), false);
			
			// send msgs from phone1 to phone2
			for (int i = 1; i <= 5; i++)
				db.sendMessage(phone2, new MessageInfo(""+(++baseMsgToken), phone1, new Date(), "Msg #" + i + " from " + phone1 + " to " + phone2), false);

			// call from phone2 to phone1
			for (int i = 1; i <= 5; i++)
				db.callPhone(phone1, new CallInfo(""+(++baseCallToken), phone2, new Date()), false);
			
			// call from phone1 to phone2
			for (int i = 1; i <= 5; i++)
				db.callPhone(phone2, new CallInfo(""+(++baseCallToken), phone1, new Date()), false);

			System.out.println("\n> After sending msgs and calls");
			db.validateUser(phone1, UDID1);
			db.validateUser(phone2, UDID2);
			
			// login to phone1 & phone2
			db.login(phone1);
			db.login(phone2);
			
			System.out.println("\n> After login");
			db.validateUser(phone1, UDID1);
			db.validateUser(phone2, UDID2);
			
			// share address book - add one user
			AddressBook addedEntries = new AddressBook();
			addedEntries.addContact(phone3, new AbContact("phone3", phone3));
			db.shareAddressBook(phone1, addedEntries, 0, 0);
			System.out.println("\n> After share AB (adding one user)");
			db.validateUser(phone1, UDID1);			
			db.validateUser(phone3, UDID3);

			// share address book - remove previous user, add two users
			addedEntries.getContacts().clear();
			addedEntries.addContact(phone4, new AbContact("phone4", phone4));
			addedEntries.addContact(phone5, new AbContact("phone5", phone5));
			db.shareAddressBook(phone1, addedEntries, 1, 0);
			System.out.println("\n> After share AB (remove previous user, add two users)");
			db.validateUser(phone1, UDID1);
			db.validateUser(phone2, UDID2);
			db.validateUser(phone3, UDID3);
			db.validateUser(phone4, UDID4);
			db.validateUser(phone5, UDID5);			
			
			// user is typing
			db.userIsTyping(phone1);
			db.userIsTyping(phone2);			

			DeviceInfo deviceInfo5 = new DeviceInfo(phone5, "capabilities", "deviceKey", "pushToken", "version", 1, 2, 3);
			AddressBook addressBook5 = new AddressBook();
			addressBook5.addContact(phone1, new AbContact("phone1", phone1));
			addressBook5.addContact(phone4, new AbContact("phone4", phone4));
			db.addUser(phone5, UDID5, deviceInfo5, addressBook5);

			System.out.println("\n> After adding phone5");
			db.validateUser(phone1, UDID1);
			db.validateUser(phone2, UDID2);
			db.validateUser(phone3, UDID3);
			db.validateUser(phone4, UDID4);
			db.validateUser(phone5, UDID5);			
		} catch (CbTestDbException e) {
			e.printStackTrace();
		} finally {
			try { 
				db.removeUser(phone1);
			} catch (CbTestDbException e) {}
			try { 
				db.removeUser(phone2);
			} catch (CbTestDbException e) {}
			try { 
				db.removeUser(phone5);
			} catch (CbTestDbException e) {}
			System.out.println("\n> After remove users");
			db.validateUser(phone1, UDID1);
			db.validateUser(phone2, UDID2);
			db.validateUser(phone3, UDID3);
			db.validateUser(phone4, UDID4);
			db.validateUser(phone5, UDID5);			
		}
	}
	
	public static void test3(DbMediator db) {
		Long basePhone = 100000000L; 
		Long abPhone = 200000000L; 
		int numPhones = 250;
		int numContacts = 250;
		try {
			// create phones
			for (int num = 0; num < numPhones; num++) {
				Long phoneNum = basePhone+num;
				String phoneStr = phoneNum.toString();
				DeviceInfo deviceInfo = new DeviceInfo(phoneStr, "capabilities", "deviceKey", "pushToken", "version", 1, 2, 3);
				AddressBook addressBook = new AddressBook();
				addressBook.addContact("1234", new AbContact("phone1234", "1234"));
				db.addUser(phoneStr, "UDID"+num, deviceInfo, addressBook);
			}
			// create address books
			long start = System.currentTimeMillis();
			for (int num = 0; num < numPhones; num++) {
				for (int i = 1; i <= numContacts; i++) {
					AddressBook addressBook = new AddressBook();
					String contactPhone = new Long(abPhone + i).toString();
					addressBook.addContact(contactPhone, new AbContact("phone"+i, contactPhone));
					db.shareAddressBook(basePhone+num+"", addressBook, 0, 0);
				}
			}
			System.out.println("Adding AB's took " + (System.currentTimeMillis() - start) + "ms");

		} catch (CbTestDbException e) {
			e.printStackTrace();
		}
		// remove phones
		for (int num = 0; num < numPhones; num++) {
			try {
				db.removeUser(basePhone+num+"");
			} catch (CbTestDbException e) {
				e.printStackTrace();
			}	
		}
	}
	
	/**
	 * @param args
	 * @throws CbTestDbException
	 */
	public static void main(String[] args) throws CbTestDbException {
		DOMConfigurator.configure("log.xml");
		CouchBaseDbMediator db = new CouchBaseDbMediator();				
		db.connectToDb();
		if (args.length > 0)
			System.out.println(db.validateUser(args[0], null));
		else
			DbMediator.test2(db);
			
		db.disconnectDb();
	}
}

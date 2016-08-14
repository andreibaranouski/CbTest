package com.cbtest.largescaletest.iteminfo;

import java.util.HashMap;
import java.util.Map;

public class AddressBook {
	Map<String, AbContact> contacts;
	
	public AddressBook() {
		this(new HashMap<String, AbContact>());
	}

	public AddressBook(Map<String, AbContact> contacts) {
		this.contacts = contacts;
	}
	
	public Map<String, AbContact> getContacts() {return contacts;}
	public AbContact getContact(String phone) {return contacts.get(phone);}
	
	public void addContact(String phone, AbContact contact) {
		contacts.put(phone, contact);
	}
	
	@Override
	public String toString() {
		return "[" + contacts.size() + " contacts] " + contacts.toString();
	}
}

package com.cbtest.largescaletest;

import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.ini4j.Options;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.cbtest.common.RandomString;
import com.cbtest.largescaletest.iteminfo.AbContact;
import com.cbtest.largescaletest.iteminfo.AddressBook;
import com.cbtest.largescaletest.iteminfo.CallInfo;
import com.cbtest.largescaletest.iteminfo.DeviceInfo;
import com.cbtest.largescaletest.iteminfo.MessageInfo;

public class DbJobProcessor implements Runnable, IApp {
	private static final String REDIS_PHONES = "PHONES";
	private static final String LOG_PROPERTIES_FILE = "log.xml";
	private static final int LOG_WATCHDOG_DELAY_MS = 30000;
	private static final String NUM_WORKER_THREADS = "4";
	private static final String REDIS_JOB_QUEUE_NAME = "DB_JOBS";
	private static final String REDIS_STATS_NUM = "STATS_NUM_";
	private static final String REDIS_STATS_SUM = "STATS_SUM_";
	private static final String REDIS_STATS_MAX = "STATS_MAX_";
	private static final String REDIS_STATS_MIN = "STATS_MIN_";
	private static final String DEV_CAP = "1";
	private static final String DEV_VER = "9";
	private static final int DEV_CID = 12345;
	private static final int SHARE_AB_NUM_CONTACTS_TO_REMOVE = 1;
	private static final long regNumStart = 111000000000L;
	private static final long notRegNumStart = 222000000000L;
	private static int notRegNumRange;
	private static int activeUserStart;
	private static int activeUserEnd;
	private static final double STATS_INTERVAL = 5.;
	protected static Logger logger = Logger.getLogger(DbJobProcessor.class);
	private static ConcurrentHashMap<DbOperationType, DbOpStat> dbOpStats = new ConcurrentHashMap<DbOperationType, DbOpStat>();
	private static DbMediator db;
	private static boolean stopped = false;
	private Jedis rdb;
	private String name;
	private Random random = new Random();
	
	public DbJobProcessor(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	protected static Jedis redisConnect() throws CbTestDbException {
		
		String redisIp;
		int redisPort;
		int redisDbIdx = 0;
		try {
			Options opt = new Options(new FileReader("dbtest.ini"));
			redisIp = opt.get("Redis1IP", "localhost");
			redisPort  = new Integer(opt.get("Redis1Port", "9999"));
		} catch ( IOException e) {
			throw new CbTestDbException("Couldn't read Redis dbtest ini file: " + e.getMessage());
		}
		
		if (redisDbIdx < 0)
			return null;
		
		logger.info("Connecting to Redis DB" + redisDbIdx);
		Jedis rdb = null;
		while (rdb == null) {
			try {
				rdb = new Jedis(redisIp, redisPort);
				rdb.select(redisDbIdx);
			} catch (Exception e) {
				rdb = null;
				sleep(1000);
				continue;
			}
		}
		logger.info("Connected to Redis DB" + redisDbIdx);
		
		Long queueSize = rdb.llen(REDIS_JOB_QUEUE_NAME);
		if (queueSize == null)
			logger.info("DB Job queue is empty");
		else
			logger.info("DB Job queue currently has " + queueSize + " items");
		return rdb;
	}
	
	protected static void sleep(int s) {
		try {
			Thread.sleep(s);
		} catch (InterruptedException e) {}
	}
	
	public long getActiveRandomPhone() {
		return random.nextInt(activeUserEnd - activeUserStart) + activeUserStart;
	}
	
	public synchronized void incStat(DbOperationType opType, long processingTime) {
		DbOpStat dbOpStat = dbOpStats.get(opType);
		if (dbOpStat == null) {
			dbOpStat = new DbOpStat(opType);
			dbOpStats.put(opType, dbOpStat);
		}
		dbOpStat.addOp(processingTime);
	}
	
	public static void showStats(Jedis rdb) {
		StringBuilder sb = new StringBuilder("DB Operation Statistics:\n");
		for (DbOpStat dbOpStat : dbOpStats.values()) {
			DbOperationType opType = dbOpStat.dbOpType;
			sb.append(opType + " - \t" + dbOpStat.numOps / STATS_INTERVAL + " per sec");
			if (dbOpStat.numOps > 0)
				sb.append("\tprocessing time [avg=" + (dbOpStat.sumOpsTime / dbOpStat.numOps) + "ms, max=" + dbOpStat.maxOpTime + "ms, min=" + dbOpStat.minOpTime + "ms]\n");
			else
				sb.append("\n");
			
			// update stats in redis for distributor 
			rdb.incrBy(REDIS_STATS_NUM + opType.toString(), dbOpStat.numOps);		
			rdb.incrBy(REDIS_STATS_SUM + opType.toString(), dbOpStat.sumOpsTime);
			
			String sMaxOpTime = rdb.get(REDIS_STATS_MAX + opType.toString());
			if (sMaxOpTime != null && !sMaxOpTime.equals("0")) {
				long maxOpTime = new Long(sMaxOpTime);
				if (dbOpStat.maxOpTime > maxOpTime)
					rdb.set(REDIS_STATS_MAX + opType.toString(), new Long(dbOpStat.maxOpTime).toString());
			} else
				rdb.set(REDIS_STATS_MAX + opType.toString(), new Long(dbOpStat.maxOpTime).toString());
			
			String sMinOpTime = rdb.get(REDIS_STATS_MIN + opType.toString());
			if (sMinOpTime != null && !sMinOpTime.equals("0")) {
				long minOpTime = new Long(sMinOpTime);
				if (dbOpStat.minOpTime < minOpTime)
					rdb.set(REDIS_STATS_MIN + opType.toString(), new Long(dbOpStat.minOpTime).toString());
			} else
				rdb.set(REDIS_STATS_MIN + opType.toString(), new Long(dbOpStat.minOpTime).toString());
			
			dbOpStat.reset();
		}
		sb.append("DB Job queue currently has " + rdb.llen(REDIS_JOB_QUEUE_NAME) + " items\n");
		sb.append("Number of active phones = " + rdb.scard(REDIS_PHONES));
		logger.info(sb.toString());		
	}


	@Override
	public void run() {
		logger.info("DbJobProcessor worker thread " + getName() + " has been started...");

		// Connect to Redis
		try {
			rdb = redisConnect();
		} catch (CbTestDbException e) {
			logger.fatal(getName() + " couldn't connect to RedisDB - " + e);
			return;
		}
		String item = null;
		Gson gson = new Gson();
		
		while(!stopped ) {
			try {
				// if this is a retry, use previous item
				item = rdb.lpop(REDIS_JOB_QUEUE_NAME);
			} catch (JedisConnectionException e) {
				logger.warn(getName() + ": Redis disconnected");
				sleep(5000);
				try {
					rdb = redisConnect();
				} catch (CbTestDbException e1) {
				}
				continue;
			} catch (Exception e) {
				logger.warn(getName() + ": Redis Error, sleeping... - " + e);
				sleep(5000);
				continue;
			}
			
			if (item == null) {
				sleep(500);
				continue;
			}
			
			DbOperations dbOps = null;
			try {				
				dbOps = gson.fromJson(item, DbOperations.class);
			} catch (JsonSyntaxException e) {		
				logger.warn(getName() + " exception when parsing db operations: " + item + " - " + e);
			}
			for (DbOperation dbOp : dbOps.dbOps) {
				long startTime = System.currentTimeMillis();
				try {
					performOperation(dbOp);
				} catch (Exception e) {
					// retry operation once more
					logger.warn(getName() + " Exception when processing db operation (retrying): " + dbOp + " - " + e + ", processed in " + (System.currentTimeMillis() - startTime) + "ms");
					sleep(500);
					try {
						startTime = System.currentTimeMillis();
						performOperation(dbOp);
						logger.info(getName() + " Operation retried and processed successfully - " + dbOp + ", processed in " + (System.currentTimeMillis() - startTime) + "ms");
					} catch (Exception e1) {
						logger.error(getName() + " Exception when processing db operation (happened again after retrying): " + dbOp + " - " + e1 + ", processed in " + (System.currentTimeMillis() - startTime) + "ms");
					}
				}
			}
		}		
		logger.info(getName() + " has been stopped");
	}

	private void performOperation(DbOperation dbOp) throws ViberDbItemExists, CbTestDbException {
		long startTime = 0;
		try {
			logger.trace(getName() + " Starting to process db operation: " + dbOp);
			startTime = System.currentTimeMillis();
			switch (dbOp.dbOpType) {
			case AddUser:
				int last2digits = new Integer(dbOp.phone.substring(dbOp.phone.length() - 2));
				DeviceInfo deviceInfo = new DeviceInfo(dbOp.phone, DEV_CAP, RandomString.getName(64), RandomString.getName(128), DEV_VER, DEV_CID, last2digits, 0);
				AddressBook addressBook = new AddressBook();
				// number of AB entries is 5 times (phone last2digits + 1)
				for (int i = 0; i < 5 * (last2digits + 1); i++) {
					if (i % 10 == 2 || i % 10 == 7) {
						// Registered contact (20%)
						long phoneNum = getActiveRandomPhone();
						addressBook.addContact(new Long(phoneNum+regNumStart).toString(), new AbContact(RandomString.getName(25), new Long(phoneNum).toString()));
					} else {
						// Unregistered contact (80%)
						long phoneNum = random.nextInt(notRegNumRange);
						addressBook.addContact(new Long(notRegNumStart + phoneNum).toString(), new AbContact(RandomString.getName(25), new Long(phoneNum).toString()));
					}
				}
				startTime = System.currentTimeMillis();	// don't count DevInfo & AB creation time
				db.addUser(dbOp.phone, dbOp.UDID, deviceInfo, addressBook);
									
				rdb.sadd(REDIS_PHONES,dbOp.phone);	// set user in Redis so we know exists 
				break;
			case RemoveUser:
				rdb.srem(REDIS_PHONES,dbOp.phone); // remove user from Redis so we know it doesn't exist anymore
				db.removeUser(dbOp.phone);
				break;
			case SendMessage:
				// remove 90% of the messages
				// the rest will be removed when login is called (getRecentMessages)
				boolean removeMsg = true;
				if (random.nextInt(10) >= 9)
					 removeMsg = false;
				db.sendMessage(dbOp.phone, new MessageInfo(RandomString.getName(25), dbOp.srcPhone, dbOp.time, dbOp.text), removeMsg);
				break;
			case CallPhone:
				// remove 90% of the calls
				// the rest will be removed when login is called (getRecentCalls)
				boolean removeCall = true;
				if (random.nextInt(10) >= 9)
					removeCall = false;
				db.callPhone(dbOp.phone, new CallInfo(RandomString.getName(25), dbOp.srcPhone, dbOp.time), removeCall);
				break;
			case Login:
				db.login(dbOp.phone);
				break;
			case ShareAb:
				AddressBook addedEntries = new AddressBook();
				int num = random.nextInt(5);
				if (num == 2) {
					// Registered contact (20%)
					long phoneNum = getActiveRandomPhone();
					addedEntries.addContact(new Long(phoneNum+regNumStart).toString(), new AbContact(RandomString.getName(25), new Long(phoneNum).toString()));
				} else {
					// Unregistered contact (80%)
					long phoneNum = random.nextInt(notRegNumRange);
					addedEntries.addContact(new Long(notRegNumStart + phoneNum).toString(), new AbContact(RandomString.getName(25), new Long(phoneNum).toString()));
				}
				startTime = System.currentTimeMillis();	// don't count AB creation time
				db.shareAddressBook(dbOp.phone, addedEntries, SHARE_AB_NUM_CONTACTS_TO_REMOVE, new Long(notRegNumStart + notRegNumRange));
				break;
			case UserIsTyping:
				db.userIsTyping(dbOp.phone);
				break;
			case ValidateUser:
				ValidationInfo validationInfo = db.validateUser(dbOp.phone, dbOp.UDID);
				if (!validationInfo.isValid)
					logger.warn(getName() + " User was not validated - " + dbOp + ": " + validationInfo);
				break;
			case GetContact:
				String UDID = db.getUdidByPhone(dbOp.phone);
				try {
					db.getRandomContact(UDID, 1);
				} catch (ViberDbItemNotFound e) {					
				}
				break;
			default:
				logger.warn(getName() + " illegal db operation: " + dbOp);
				break;
			}
			
			long processingTime = System.currentTimeMillis() - startTime;
			logger.debug(getName() + " " + dbOp.dbOpType + " processed in " + processingTime + "ms");
			incStat(dbOp.dbOpType, processingTime);
		} catch (ViberDbItemNotFound e) {
			if (rdb.sismember(REDIS_PHONES, dbOp.phone))
				logger.warn(getName() + " Error processing db operation even though phone exists: " + dbOp + " - " + e);
			else
				logger.debug(getName() + " Error processing db operation because phone does not exist: " + dbOp + " - " + e);
		} catch (ViberDbItemExists e) {
			logger.warn(getName() + " Error processing db operation: " + dbOp + " - " + e);
		} catch (NumberFormatException e) {
			logger.warn(getName() + " NumberFormatException when processing db operation: " + dbOp + " - " + e);
		} 
	}

	public static void main(String[] args) throws InterruptedException {	
		DOMConfigurator.configureAndWatch(LOG_PROPERTIES_FILE, LOG_WATCHDOG_DELAY_MS);

		// connect to CouchBaseDB
		db = new CouchBaseDbMediator();
		try {
			db.connectToDb();
		} catch (CbTestDbException e) {
			logger.fatal("Couldn't connect to CouchBaseDB - " + e);
			return;
		}

		try {
			Options opt = new Options(new FileReader("dbtest.ini"));

			int numWorkerThreads = new Integer(opt.get("NumWorkerThreads", NUM_WORKER_THREADS));
			notRegNumRange = new Integer(opt.get("DbJobDist_NotRegNumRange","2000000000"));
			activeUserStart = new Integer(opt.get("DbJobDist_ActiveUserStart","0"));
			activeUserEnd = new Integer(opt.get("DbJobDist_ActiveUserEnd","999999999"));
			
			for (int i = 0; i < numWorkerThreads; i++) {
				DbJobProcessor jobProcessor = new DbJobProcessor("DbJobProcessor #" + (i + 1));
				ShutdownInterceptor shutdownInterceptor = new ShutdownInterceptor(jobProcessor);
				Runtime.getRuntime().addShutdownHook(shutdownInterceptor);

				new Thread(jobProcessor).start();
			}		

		} catch ( IOException e) {
			logger.error("Couldn't read dbtest ini file: " + e.getMessage());
			return;
		}		

		// Connect to Redis
		Jedis rdb;
		try {
			rdb = redisConnect();
		} catch (CbTestDbException e) {
			logger.fatal("Couldn't connect to RedisDB - " + e);
			return;
		}
		
		// show stats every interval
		while (true) {
			Thread.sleep((long) (STATS_INTERVAL * 1000));
			showStats(rdb);
		}
	}

	@Override
	public void shutDown() {
		logger.info("Stopping Job Processor");
		DbJobProcessor.stopped = true;
	}

	@Override
	public void start() {
	}
}

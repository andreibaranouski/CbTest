package com.cbtest.largescaletest;

import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.ini4j.Options;

import redis.clients.jedis.Jedis;

import com.google.gson.Gson;
import com.cbtest.common.RandomString;

public class DbJobDistributor implements Runnable {
	private static final int UDID_LEN = 40;
	private static final String LOG_PROPERTIES_FILE = "log.xml";
	private static final int LOG_WATCHDOG_DELAY_MS = 30000;
	private static final String REDIS_JOB_QUEUE_NAME = "DB_JOBS";
	private static final String REDIS_FIRST_USER_PHONE = "RedisFirstUserPhone";
	private static final String REDIS_LAST_USER_PHONE = "RedisLastUserPhone";
	private static final String REDIS_STATS_NUM = "STATS_NUM_";
	private static final String REDIS_STATS_SUM = "STATS_SUM_";
	private static final String REDIS_STATS_MAX = "STATS_MAX_";
	private static final String REDIS_STATS_MIN = "STATS_MIN_";
	protected static Logger logger = Logger.getLogger(DbJobDistributor.class);
	private static final long regNumStart = 111000000000L;
	private static final double STATS_INTERVAL = 10.;
	private static final double MAX_OPS_PER_THREAD = 500;
	private static int[] dbOpStats = new int[DbOperationType.values().length];
	private static Long maxQueueSize;
	private static Long maxUserRange;
	private static Long maxActiveUsers;
	private static Long activeUserStart;
	private static Long activeUserEnd;
	private static Object queueLock = new Object();
	private static boolean pauseOperations;
	private static long redisFirstUserPhone = 0;
	private static long redisLastUserPhone = 0;
	private static int numPhonesInDb = 0;
	private static String iniFileName;
	
	private Jedis rdb = null;
	private Gson gson = new Gson();
	private DbOperationType dbOpType;
	private double dbOpFreq = 0;
	private Random random = new Random();
	
	public DbJobDistributor(DbOperationType dbOpType, double dbOpFreq) {
		this.dbOpType = dbOpType;
		this.dbOpFreq = dbOpFreq;
	}
	
	protected static Jedis redisConnect() throws CbTestDbException {
		String redisIp;
		int redisPort;
		int redisDbIdx = 0;
		try {
			Options opt = new Options(new FileReader(iniFileName));
			redisIp = opt.get("Redis1IP", "localhost");
			redisPort  = new Integer(opt.get("Redis1Port", "9999"));
		} catch ( IOException e) {
			throw new CbTestDbException("Couldn't read Redis dbtest ini file: " + e.getMessage());
		}
		
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
			
		return rdb;
	}
	
	public synchronized void incStat(DbOperationType opType, int num) {
		dbOpStats[opType.ordinal()]+=num;
	}

	public static void showStats(Jedis rdb) {
		StringBuilder sb = new StringBuilder("DB Operation Distribution Statistics:\n");
		for (int i = 0; i < DbOperationType.values().length; i++) {
			DbOperationType opType = DbOperationType.values()[i];

			// get combined processing stats from redis
			try {
				long numOps = new Long(rdb.get(REDIS_STATS_NUM + opType.toString()));		
				long sumOpsTime = new Long(rdb.get(REDIS_STATS_SUM + opType.toString()));		
				long maxOpTime = new Long(rdb.get(REDIS_STATS_MAX + opType.toString()));		
				long minOpTime = new Long(rdb.get(REDIS_STATS_MIN + opType.toString()));

				if (dbOpStats[i] == 0 && numOps == 0)
					continue;
				
				sb.append(opType + " - \t" + dbOpStats[i] / STATS_INTERVAL + " per sec");
				dbOpStats[i] = 0;

				if (numOps > 0)
					sb.append("\tprocessing time [" + (numOps / STATS_INTERVAL) + " per sec, avg=" + (sumOpsTime / numOps) + "ms, max=" + maxOpTime + "ms, min=" + minOpTime + "ms]\n");
				else
					sb.append("\n");
			} catch (NullPointerException | NumberFormatException e) {
				sb.append("\n");
			}
			
			// reset redis stats
			rdb.set(REDIS_STATS_NUM + opType.toString(), "0");
			rdb.set(REDIS_STATS_SUM + opType.toString(), "0");
			rdb.set(REDIS_STATS_MAX + opType.toString(), "0");
			rdb.set(REDIS_STATS_MIN + opType.toString(), "0");
		}
		
		long jobQueueSize = rdb.llen(REDIS_JOB_QUEUE_NAME);
		long numActivePhones = rdb.scard("PHONES");
		sb.append("DB Job queue currently has " + jobQueueSize + " items\n");
		sb.append("Number of active phones = " + numActivePhones);
		logger.info(sb.toString());

		// pause/continue operations according to queue size
		if (jobQueueSize > maxQueueSize) {
			logger.info("Pausing job queue distribution, queue size is " + jobQueueSize);
			pauseOperations = true;
		} else if (pauseOperations) {
			logger.info("Continuing job queue distribution");
			pauseOperations = false;
			synchronized (queueLock) {
				queueLock.notifyAll();
			}			
		} 
		
		// stop processing if reaching maximum user range
		long firstPhone = new Long(rdb.get(REDIS_FIRST_USER_PHONE));
		long lastPhone = new Long(rdb.get(REDIS_LAST_USER_PHONE));
		if (numActivePhones > maxActiveUsers || (lastPhone - firstPhone) > maxUserRange) {
			String msg = "Reached maximum number of users, number of active users =  " + numActivePhones + ", next phone is " + lastPhone + ", first phone is " + firstPhone + " - Exiting...";
			logger.fatal(msg);
			System.out.println(msg);
			System.exit(2);
		}
	}

	synchronized public Long getNextPhone() {
		redisLastUserPhone = rdb.incr(REDIS_LAST_USER_PHONE);
		numPhonesInDb = (int)(redisLastUserPhone - redisFirstUserPhone);
		return redisLastUserPhone;
	}
	
	synchronized public Long getNextPhoneForRemoval() {
		redisFirstUserPhone = rdb.incr(REDIS_FIRST_USER_PHONE);
		numPhonesInDb = (int)(redisLastUserPhone - redisFirstUserPhone);
		return redisFirstUserPhone-1;
	}
	
	public String getActiveRandomPhone() {
		return new Long(random.nextInt((int)(activeUserEnd - activeUserStart)) + redisFirstUserPhone + activeUserStart).toString();
	}
	
	public String getExistingActiveRandomPhone() throws IndexOutOfBoundsException {
		if (numPhonesInDb < activeUserStart)
			throw new IndexOutOfBoundsException();
		return new Long(random.nextInt((int)(Math.min(numPhonesInDb,activeUserEnd) - activeUserStart)) + redisFirstUserPhone + activeUserStart).toString();
	}

//	public String getRandomPhone() {
//		return new Long(random.nextInt((int) (Math.max(1, Math.min(numPhonesInDb, maxActiveUserRange)))) + redisFirstUserPhone).toString();
//		// get random phone until retrieved one that is not going to be removed
//		while (true) {			
//			String phone = rdb.srandmember("PHONES");
//			if (phone != null && new Long(phone) > redisFirstUserPhone)
//				return phone;
//		}
//	}
	
	protected static void sleep(int s) {
		try {
			Thread.sleep(s);
		} catch (InterruptedException e) {}
	}
	
	public void addDbOperationsToQueue(DbOperations dbOps) {
		rdb.rpush(REDIS_JOB_QUEUE_NAME, gson.toJson(dbOps));
		logger.debug("Added operations " + dbOps);
	}

	public void createDbOperation(DbOperationType dbOpType, int numOps) {
		DbOperations dbOps = new DbOperations();
		try {
			for (int i = 0; i < numOps; i++)
				switch (dbOpType) {
				case AddUser:			
					dbOps.addDbOp(new DbOperation(DbOperationType.AddUser, getNextPhone().toString(), RandomString.getName(UDID_LEN), 
													new Date(), null, null));
					break;
				case RemoveUser:
					dbOps.addDbOp(new DbOperation(DbOperationType.RemoveUser, getNextPhoneForRemoval().toString()));
					break;
				case SendMessage:
					String srcPhone = getExistingActiveRandomPhone();
					String toPhone = getExistingActiveRandomPhone();
					dbOps.addDbOp(new DbOperation(DbOperationType.SendMessage, toPhone, null, new Date(), srcPhone, 
															"Test message sent from " + srcPhone + " to " + toPhone));
					break;
				case CallPhone:
					dbOps.addDbOp(new DbOperation(DbOperationType.CallPhone, getExistingActiveRandomPhone(), null, new Date(), getExistingActiveRandomPhone(), null));
					break;
				case Login:
					dbOps.addDbOp(new DbOperation(DbOperationType.Login, getExistingActiveRandomPhone()));
					break;
				case ShareAb:
					dbOps.addDbOp(new DbOperation(DbOperationType.ShareAb, getActiveRandomPhone(), null, new Date(), null, null));
					break;
				case UserIsTyping:
					dbOps.addDbOp(new DbOperation(DbOperationType.UserIsTyping, getExistingActiveRandomPhone()));
					break;
				case ValidateUser:
					dbOps.addDbOp(new DbOperation(DbOperationType.ValidateUser, getExistingActiveRandomPhone()));
					break;
				case GetContact:
					dbOps.addDbOp(new DbOperation(DbOperationType.GetContact, getExistingActiveRandomPhone()));
					break;
				default:
					break;			
				}
			addDbOperationsToQueue(dbOps);
			incStat(dbOpType, numOps);
		} catch (IndexOutOfBoundsException e) {
			logger.warn("Couldn't create " + numOps + " " + dbOpType + " DB operations because existing phones have not reached active phones range");
		}
	}

	@Override
	public void run() {
		try {
			if (dbOpFreq <= 0)
				return;
			
			logger.info("Starting thread to process " + dbOpType + " at " + dbOpFreq + " per sec");
			
			try {
				rdb = redisConnect();
			} catch (CbTestDbException e) {
				logger.fatal("Couldn't connect to RedisDB - " + e);
				return;
			}
			
			// create initial user phone key
			String firstPhone = rdb.get(REDIS_FIRST_USER_PHONE);
			logger.info("Redis first phone is intially " + firstPhone);
			if (firstPhone == null) {
				firstPhone = new Long(regNumStart).toString();
				rdb.set(REDIS_FIRST_USER_PHONE, firstPhone);
			}
			if (redisFirstUserPhone == 0)
				redisFirstUserPhone = new Long(firstPhone);
			String lastPhone = rdb.get(REDIS_LAST_USER_PHONE);
			logger.info("Redis last phone is intially " + lastPhone);
			if (lastPhone == null) {
				lastPhone = new Long(firstPhone).toString(); 
				rdb.set(REDIS_LAST_USER_PHONE, lastPhone);
			}
			if (redisLastUserPhone == 0)
				redisLastUserPhone = new Long(lastPhone);
			
			numPhonesInDb = (int)(redisLastUserPhone - redisFirstUserPhone);
			
			if (dbOpFreq > 1) {
				while (true) {
					if (pauseOperations) {
						logger.debug("Pausing operation until job queue empties");
						synchronized (queueLock){
							queueLock.wait();	
						}						
						logger.debug("Continuing operation to job queue");
					}
					int opGroupSize = 1;
					if (dbOpFreq >= 100)
						opGroupSize = 10;
					long start = System.currentTimeMillis();
					for (int i=0; i < dbOpFreq; i+=opGroupSize)
						createDbOperation(dbOpType, (int) Math.min(opGroupSize, dbOpFreq - i));
					long timePassed = System.currentTimeMillis() - start;					
					if (timePassed > 1000)
						logger.warn("Creation of " + dbOpType + " jobs is too slow, took " + timePassed + "ms to create " + dbOpFreq + " jobs");
					else
						Thread.sleep(1000 - timePassed);
				}				
			} else {
				int interval = (int)(1000.0/dbOpFreq);
				while (true) {
					if (pauseOperations) {
						logger.debug("Pausing operation until job queue empties");
						synchronized (queueLock){
							queueLock.wait();	
						}						
						logger.debug("Continuing operation to job queue");
					}
					long start = System.currentTimeMillis();
					createDbOperation(dbOpType, 1);
					long timePassed = System.currentTimeMillis() - start;
					if (timePassed > interval)
						logger.warn("Creation of " + dbOpType + " jobs is too slow, took " + timePassed + "ms to create " + dbOpFreq + " jobs");
					Thread.sleep(interval - timePassed);
				}
			}			
		} catch (InterruptedException e) {
			logger.warn("JobDistributor thread for operation " + dbOpType + " has been interrupted");
		}
	}

	public static void main(String[] args) throws InterruptedException {
		DOMConfigurator.configureAndWatch(LOG_PROPERTIES_FILE, LOG_WATCHDOG_DELAY_MS);
		
		if (args.length == 1)
			iniFileName = args[0];
		else
			iniFileName = "dbtest.ini";
		
		try {
			Options opt = new Options(new FileReader(iniFileName));
			maxQueueSize = new Long(opt.get("DbJobDist_MaxQueueSize","9999999"));
			maxUserRange = new Long(opt.get("DbJobDist_MaxUserRange","999999999"));
			maxActiveUsers = new Long(opt.get("DbJobDist_MaxActiveUsers","999999999"));
			activeUserStart = new Long(opt.get("DbJobDist_ActiveUserStart","0"));
			activeUserEnd = new Long(opt.get("DbJobDist_ActiveUserEnd","999999999"));
			
			for (DbOperationType opType : DbOperationType.values()) {
				// create operation threads
				// each thread can process up to MAX_OPS_PER_THREAD
				double ops = new Double(opt.get("DbOp_" + opType, "0."));
				while (ops > 0) {
					double threadOps = Math.min(ops,MAX_OPS_PER_THREAD);
					new Thread(new DbJobDistributor(opType, threadOps)).start();
					ops -= MAX_OPS_PER_THREAD;
				}
			}
		} catch ( IOException | NumberFormatException e) {
			logger.fatal("Couldn't read " + iniFileName + " ini file: " + e.getMessage());
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
}


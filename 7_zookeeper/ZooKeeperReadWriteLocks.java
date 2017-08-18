import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperReadWriteLocks {
	
	// Total number of workers
	private static int NUM_READERS = 10;
	private static int NUM_WRITERS = 3;
	
	// ZooKeeper's parameters
	public static String HOST = "127.0.0.1:2181";
	public static int SESSION_TIMEOUT = 5000;
	
	// ZooKeeper path representing the shared area that readers and writers will use
	public static String LOCKS_PATH = "/locks";
	
	// Connect to ZooKeeper
	public ZooKeeper connect() throws IOException, InterruptedException {
		CountDownLatch waitingLatch = new CountDownLatch(1);

		ZooKeeper zk = new ZooKeeper(HOST, SESSION_TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
					waitingLatch.countDown();
				}
			}
		});

		// Waits until this thread is connected
		waitingLatch.await();
		return zk;
	}

	// Close connection to ZooKeeper
	public void close(ZooKeeper zk) throws InterruptedException {
		zk.close();
	}

	// Read-write locks
	public class ReadWriteLock {
		private String name;
		private String lockPath;
		private ZooKeeper zk;
		
		// Create new lock
		public ReadWriteLock(ZooKeeper zk, String name) throws InterruptedException, KeeperException {
			this.name = name;
			this.zk = zk;
			this.lockPath = LOCKS_PATH + "/" + this.name;
			
			// Create the path /locks in ZooKeeper's tree in case it doesn't exist
			Stat locksPathExists = zk.exists(LOCKS_PATH, false);
			
			if (locksPathExists == null) {
				this.zk.create(LOCKS_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			// Create the node /locks/currentLock in ZooKeeper's tree
			this.zk.create(this.lockPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		public void getReadLock(String id) throws InterruptedException, KeeperException {
			System.out.println("Reader " + id + " about to acquire read lock.");
			
			// Add the node to the lock queue
			String thisNodePath = this.zk.create(this.lockPath + "/read-" + id + "-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			
			while (true) {
				CountDownLatch waitingLatch = new CountDownLatch(1);
				synchronized (waitingLatch) {
					// Check the queue, if there are no writers with a lower sequential number then acquire the lock
					List<String> queue = zk.getChildren(this.lockPath, new Watcher() {
						public void process(WatchedEvent event) {
							waitingLatch.countDown();
						}
					});
					
					boolean writerBeforeMe = false;
					for (String queueElement : queue) {
						long queueInteger = Long.parseLong(queueElement.split("-")[2]);
						long thisElementInteger = Long.parseLong(thisNodePath.split("-")[2]);
						if (queueInteger < thisElementInteger && queueElement.startsWith("write")) {
							writerBeforeMe = true;
							break;
						}
					}
					
					if (writerBeforeMe) {
						// There is a writer before this node in the queue, sleep
						waitingLatch.await();
					} else {
						// Acquire the lock
						System.out.println("Reader " + id + " adquired read lock.");
						return;
					}
				}
			}
			
		}
		
		public void getWriteLock(String id) throws InterruptedException, KeeperException {
			System.out.println("Writer " + id + " about to acquire write lock.");
			
			// Add the node to the lock queue
			this.zk.create(this.lockPath + "/write-" + id + "-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			
			while (true) {
				CountDownLatch waitingLatch = new CountDownLatch(1);
				synchronized (waitingLatch) {
					// Check the queue, if the element with the smaller sequential number is this then acquire the lock
					List<String> queue = zk.getChildren(this.lockPath, new Watcher() {
						public void process(WatchedEvent event) {
							waitingLatch.countDown();
						}
					});
					
					// Find the minimum element and check if that one is this
					String minElement = queue.get(0);
					
					for (int i = 1; i < queue.size(); i++) {
						String queueElement = queue.get(i);
						long queueInteger = Long.parseLong(queueElement.split("-")[2]);
						long minInteger = Long.parseLong(minElement.split("-")[2]);
						if (queueInteger < minInteger) {
							minElement = queueElement;
						}
					}
					
					if (minElement.split("-")[1].equals(id)) {
						// Acquire the lock
						System.out.println("Writer " + id + " adquired write lock.");
						return;
					} else {
						waitingLatch.await();
					}
				}
			}
		}
		
		public void returnLock(String id) throws InterruptedException, KeeperException {
			// Find the lock and delete it
			List<String> queue = zk.getChildren(this.lockPath, false);
			
			for (String queueElement : queue) {
				String[] splittedQueueElement = queueElement.split("-");
				
				if (splittedQueueElement[1].equals(id)) {
					System.out.println("Worker " + id + " released lock.");
					zk.delete(this.lockPath + "/" + queueElement, -1);
					break;
				}
			}
			 
		}
		
		public void deleteLock(ZooKeeper zk) throws KeeperException, InterruptedException {
			zk.delete(this.lockPath, -1);
		}
	}
	
	public class WorkerReader implements Runnable {
		private ReadWriteLock lock;
		
		public WorkerReader(ReadWriteLock lock) {
			this.lock = lock;
		}
		
		public void run() {
			long workerId = Thread.currentThread().getId();
			
			try {
				// Connect to ZooKeeper
				ZooKeeper zk = ZooKeeperReadWriteLocks.this.connect();

				if (zk != null) {
					// Do something
					Thread.sleep((long)(Math.random() * 5000));
					
					// Get a read lock
					this.lock.getReadLock("" + workerId);
					
					// Do something
					Thread.sleep((long)(Math.random() * 5000));
					
					// Release the lock
					this.lock.returnLock("" + workerId);
					
					// Close the connection
					ZooKeeperReadWriteLocks.this.close(zk);
					
				} else {
					System.out.println("ERROR: Worker " + workerId + " could not connect to ZooKeeper.");
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public class WorkerWriter implements Runnable {
		private ReadWriteLock lock;
		
		public WorkerWriter(ReadWriteLock lock) {
			this.lock = lock;
		}
		
		public void run() {
			long workerId = Thread.currentThread().getId();
			
			try {
				// Connect to ZooKeeper
				ZooKeeper zk = ZooKeeperReadWriteLocks.this.connect();

				if (zk != null) {
					// Do something
					Thread.sleep((long)(Math.random() * 5000));
					
					// Get a write lock
					this.lock.getWriteLock("" + workerId);
					
					// Do something
					Thread.sleep((long)(Math.random() * 5000));
					
					// Release the lock
					this.lock.returnLock("" + workerId);
					
					// Close the connection
					ZooKeeperReadWriteLocks.this.close(zk);

				} else {
					System.out.println("ERROR: Worker " + workerId + " could not connect to ZooKeeper.");
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		ZooKeeperReadWriteLocks z = new ZooKeeperReadWriteLocks();
		
		// Connect to ZooKeeper to create the lock
		try {
			ZooKeeper zk = z.connect();

			if (zk == null) {
				System.out.println("ERROR: Could not connect to ZooKeeper.");
				return;
			}
			
			ReadWriteLock lock = z.new ReadWriteLock(zk, "lock");

			List<Thread> workers = new ArrayList<Thread>();
			
			for (int i = 0; i < NUM_READERS; i++) {
				Thread writer = new Thread(z.new WorkerReader(lock));
				writer.start();
				workers.add(writer);
			}
			
			for (int i = 0; i < NUM_WRITERS; i++) {
				Thread reader = new Thread(z.new WorkerWriter(lock));
				reader.start();
				workers.add(reader);
			}

			for (Thread t : workers) {
				t.join();
			}

			lock.deleteLock(zk);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


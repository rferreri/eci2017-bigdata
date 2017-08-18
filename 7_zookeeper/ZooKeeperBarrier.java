import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperBarrier implements Runnable {

	// Total number of threads executing
	public static int NUM_THREADS = 10;

	// Number of threads needed to release the barrier
	public static int LIMIT_BARRIER = 10;

	// ZooKeeper's host and port
	public static String HOST = "127.0.0.1:2181";
	public static int SESSION_TIMEOUT = 5000;

	// Barrier nodes
	public static String BARRIER_ROOT = "/barrier";
	public static String BARRIER_CHILDREN = "/barrier/children";
	public static String BARRIER_LIFTED = "/barrier/lifted";

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

	// Join the barrier
	public void barrier(ZooKeeper zk, String name) throws KeeperException, InterruptedException {
		// If the barrier nodes do not exist, create them
		Stat rootNodeExists = zk.exists(BARRIER_ROOT, false);
		Stat childrenNodeExists = zk.exists(BARRIER_CHILDREN, false);
		
		if (rootNodeExists == null || childrenNodeExists == null) {
			try {
				zk.create(BARRIER_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				zk.create(BARRIER_CHILDREN, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException ex) {
				// Two nodes trying to create the barrier nodes at the same time, don't do anything
			}
		}

		// At this point both barrier nodes are guaranteed to exist, we now join the barrier by adding a node to the children subtree
		String nodeName = zk.create(BARRIER_CHILDREN + "/thread-" + name + "-seq-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		while (true) {
			CountDownLatch waitingLatch = new CountDownLatch(1);

			synchronized(waitingLatch) {
				// First of all we test if the barrier has been lifted by another node
				Stat barrierLifted = zk.exists(BARRIER_LIFTED, new Watcher() {
					public void process(WatchedEvent event) {
						waitingLatch.countDown();
					}
				});
				
				if (barrierLifted != null) {
					// The barrier has been lifted, we break
					break;
					
				} else {
					// The barrier has not been lifted yet
					List<String> barrierList = zk.getChildren(BARRIER_CHILDREN, false);
					
					// If the barrier is full then we must lift it
					if (barrierList.size() >= LIMIT_BARRIER) {
						try {
							zk.create(BARRIER_LIFTED, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						} catch (KeeperException e) {
							// This could happen if two threads tried to lift the barrier at the same time
						}
						break;
					}
					
					// If the barrier is not full and has not been lifted then we must wait
					waitingLatch.await();
				}
			}
		}
		
		// At this point we must delete the subtree to guarantee that the barrier can be used again
		// Each thread deletes its own node and the last one deletes the rest, cleans up and free the rest
		zk.delete(nodeName, -1);
		
		while (true) {
			CountDownLatch waitingLatch = new CountDownLatch(1);
			
			synchronized(waitingLatch) {
				Stat nodesAreFree = zk.exists(BARRIER_ROOT, new Watcher() {
					public void process(WatchedEvent event) {
						waitingLatch.countDown();
					}
				});
				
				if (nodesAreFree == null) {
					// The barrier has been cleaned, threads may continue
					break;
				} else {
					// The barrier has not been cleaned yet, we check the number of nodes
					try {
						List<String> barrierList = zk.getChildren(BARRIER_CHILDREN, false);
					
						// If the barrier empty then we must clean it
						if (barrierList.size() == 0) {
							try {
								zk.delete(BARRIER_LIFTED, -1);
								zk.delete(BARRIER_CHILDREN, -1);
								zk.delete(BARRIER_ROOT, -1);
							} catch (KeeperException e) {
								// This could happen if two threads tried to clean at the same time
							}
							break;
						}
					
						// If the barrier is not empty yet, the thread must wait
						waitingLatch.await();
					} catch (KeeperException ex) {
						// Another thread deleted the main node
					}
				}
			}
		}
	}

	// Thread execution
	public void run() {
		long threadId = Thread.currentThread().getId();

		try {
			ZooKeeper zk = this.connect();

			if (zk != null) {

				// Test the barrier 10 times
				for (int i = 0; i < 10; i++) {
					// Do some work
					Thread.sleep((long)(Math.random() * 5000));
					
					// Synchronize with the rest of threads
					System.out.println("Thread " + threadId + " entered the barrier " + i + ".");
					
					this.barrier(zk, "" + threadId);
					
					System.out.println("Thread " + threadId + " exited the barrier " + i + ".");
				}

			} else {
				System.out.println("ERROR: Thread " + threadId + " could not connect to ZooKeeper.");
			}
			
			this.close(zk);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i < NUM_THREADS; i++) {
			(new Thread(new ZooKeeperBarrier())).start();
		}
	}

}
import java.io.IOException;
import java.nio.ByteBuffer;
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

public class ZooKeeperProducerConsumer {

	// Number of producers and consumers executing
	public static int NUM_PRODUCERS = 5;
	public static int NUM_CONSUMERS = 5;
	
	// Items to produce by the producer
	public static int ITEMS_TO_PRODUCE = 5;
	
	// ZooKeeper's host and port
	public static String HOST = "127.0.0.1:2181";
	public static int SESSION_TIMEOUT = 5000;
	
	// Queue node in ZooKeeper's tree
	public static String QUEUE_ROOT = "/queue";
	public static String PRODUCERS_CHECK = "/producers";
	
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
	
	public class Producer implements Runnable {
		public void registerProducer(ZooKeeper zk, long producerId) throws KeeperException, InterruptedException {
			Stat producersNodeExists = zk.exists(PRODUCERS_CHECK, false);
			
			if (producersNodeExists == null) {
				try {
					zk.create(PRODUCERS_CHECK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} catch (KeeperException ex) {
					// Two producers trying to create the root node at the same time, don't do anything
				}
			}
			
			// Create ephemeral node for this producer
			zk.create(PRODUCERS_CHECK + "/", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		}
		
		public void createQueue(ZooKeeper zk) throws KeeperException, InterruptedException {
			// If the queue node does not exist, create it
			Stat queueNodeExists = zk.exists(QUEUE_ROOT, false);
			
			if (queueNodeExists == null) {
				try {
					zk.create(QUEUE_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} catch (KeeperException ex) {
					// Two nodes trying to create the queue root node at the same time, don't do anything
				}
			}
		}
		
		public void addToQueue(ZooKeeper zk, int product) throws KeeperException, InterruptedException {
			byte[] bytes = ByteBuffer.allocate(4).putInt(product).array();
			
			zk.create(QUEUE_ROOT + "/product", bytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		}
		
		public void run() {
			long producerId = Thread.currentThread().getId();
			
			try {
				ZooKeeper zk = ZooKeeperProducerConsumer.this.connect();

				if (zk != null) {
					// Register 
					this.registerProducer(zk, producerId);
					
					// Create the queue
					this.createQueue(zk);
					
					for (int i = 0; i < ZooKeeperProducerConsumer.ITEMS_TO_PRODUCE; i++) {
						// Wait random time
						Thread.sleep((long)(Math.random() * 2500));
						
						// Produce one element
						int produce = (int) Math.round(Math.random() * 100);
						
						// Add the element to the queue
						this.addToQueue(zk, produce);
						
						System.out.println("Producer " + producerId + " produced item " + produce + ".");
					}
					
					ZooKeeperProducerConsumer.this.close(zk);
					System.out.println("Producer " + producerId + " exited.");
				} else {
					System.out.println("ERROR: Producer " + producerId + " could not connect to ZooKeeper.");
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public class Consumer implements Runnable {
		public int consume(ZooKeeper zk) throws InterruptedException, KeeperException {
			
			// Check that the queue exists
			while (true) {
				CountDownLatch waitingLatch = new CountDownLatch(1);
				
				synchronized(waitingLatch) {
					Stat queueExists = zk.exists(QUEUE_ROOT, new Watcher() {
						public void process(WatchedEvent event) {
							waitingLatch.countDown();
						}
					});
					
					if (queueExists != null) {
						break;
					}
					
					// Wait until the queue is created
					waitingLatch.await();
				}
			}
			
			// Consume elements
			while (true) {
				List<String> queue = zk.getChildren(QUEUE_ROOT, false);
					
				if (queue.size() == 0) {
					// The queue is empty, check if there are producers registered
					List<String> producers = zk.getChildren(PRODUCERS_CHECK, false);
					
					if (producers.size() == 0) {
						// No producers left, finish
						return -1;
					}
				} else {
					// Consume the element that has the minimum id
					String minElement = queue.get(0);
					
					for (int i = 1; i < queue.size(); i++) {
						String queueElement = queue.get(i);
						long queueInteger = Long.parseLong(queueElement.substring(7));
						long minInteger = Long.parseLong(minElement.substring(7));
						if (queueInteger < minInteger) {
							minElement = queueElement;
						}
					}
					
					// Try to consume this element
					try {
						Stat stat = null;
						byte [] productInBytes = zk.getData(QUEUE_ROOT + "/" + minElement, false, stat);
						
						zk.delete(QUEUE_ROOT + "/" + minElement, -1);
						
						return ByteBuffer.wrap(productInBytes).getInt();
						
					} catch (KeeperException ex) {
						// Another consumer consumed this element, try again with another element
					}
					
				}
			}
		}
		
		public void run() {
			long consumerId = Thread.currentThread().getId();
			
			try {
				ZooKeeper zk = ZooKeeperProducerConsumer.this.connect();

				if (zk != null) {
					while (true) {
						// Consume element
						int consumed = this.consume(zk);

						if (consumed == -1) {
							// No more producers and queue is empty
							break;
						}
						
						System.out.println("Consumer " + consumerId + " consumed item " + consumed + ".");
					}
					
					ZooKeeperProducerConsumer.this.close(zk);
					System.out.println("Consumer " + consumerId + " exited.");

				} else {
					System.out.println("ERROR: Consumer " + consumerId + " could not connect to ZooKeeper.");
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main (String[] args) {
		ZooKeeperProducerConsumer z = new ZooKeeperProducerConsumer();
		
		for (int i = 0; i < NUM_PRODUCERS; i++) {
			(new Thread(z.new Producer())).start();
		}
		
		for (int i = 0; i < NUM_CONSUMERS; i++) {
			(new Thread(z.new Consumer())).start();
		}
	}
	
}

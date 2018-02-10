package me.zhaoyudong;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DistributedLock {

    private ZooKeeper zooKeeper;
    private String lockPath;
    private LockWatcher lockWatcher;
    private List<ACL> acls;
    private CountDownLatch latch;
    private String currentLockId;


    public DistributedLock(ZooKeeper zooKeeper, String lockPath) {
        this(zooKeeper, lockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public DistributedLock(ZooKeeper zooKeeper, String lockPath, List<ACL> acls) {
        this.zooKeeper = zooKeeper;
        this.lockPath = lockPath;
        this.acls = acls;
        this.latch = new CountDownLatch(1);
    }

    private synchronized void prepare() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(this.lockPath, false) == null) {
            createParentPath(zooKeeper, acls, lockPath);
        }
        this.currentLockId = zooKeeper.create(lockPath + "/node_", null, acls, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.lockWatcher = new LockWatcher();

    }

    private void createParentPath(ZooKeeper zooKeeper, List<ACL> acls, String lockPath) throws KeeperException, InterruptedException {
        int lastIndex = lockPath.lastIndexOf("/");
        if (lastIndex > 0) {
            createParentPath(zooKeeper, acls, lockPath.substring(0, lastIndex));
        } else {
            zooKeeper.create(lockPath, null, acls, CreateMode.PERSISTENT);
        }

    }

    public synchronized void lock() throws Exception {
        if (this.currentLockId != null) {
            throw new Exception("already trying");
        }
        try {
            prepare();
            lockWatcher.checkLock();
            latch.await();
        } catch (InterruptedException e) {
            cleanup();
        }
    }

    public synchronized boolean trylock(long timeout, TimeUnit timeUnit) throws Exception {
        if (this.currentLockId != null) {
            throw new Exception("already trying");
        }
        try {
            prepare();
            lockWatcher.checkLock();
            boolean success = latch.await(timeout, timeUnit);
            if (success) {
                return true;
            }

        } catch (InterruptedException e) {
            cleanup();
        }
        return false;
    }

    public synchronized void unlock() {
        if (currentLockId == null)
            return;
        cleanup();
    }

    private void cleanup() {
        this.currentLockId = null;
        this.lockWatcher = null;
        this.latch = new CountDownLatch(1);
    }

    private class LockWatcher implements Watcher {

        public void checkLock() {
            try {
                List<String> children = zooKeeper.getChildren(lockPath, null);
                children.sort((o1, o2) -> {
                    int val1 = Integer.parseInt(o1.replace("node_", ""));
                    int val2 = Integer.parseInt(o2.replace("node_", ""));
                    return val1 - val2;
                });
                int index = children.indexOf(currentLockId.replace(lockPath + "/", ""));
                if (index == 0) {
                    latch.countDown();
                } else {
                    String nextLowestNode = children.get(index - 1);
                    Stat stat = zooKeeper.exists(lockPath + "/" + nextLowestNode, this);
                    if (stat == null)
                        checkLock();
                }
            } catch (KeeperException e) {
                e.printStackTrace();
                cleanup();
            } catch (InterruptedException e) {
                e.printStackTrace();
                cleanup();
            }
        }

        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getState().equals(Event.KeeperState.Expired)) {
                cleanup();
            } else if (watchedEvent.getState().equals(Event.EventType.NodeDeleted)) {
                checkLock();
            }
        }
    }


}

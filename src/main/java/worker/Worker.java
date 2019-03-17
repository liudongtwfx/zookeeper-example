package worker;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
public class Worker {
    private static final List<String> onGoingTasks = new ArrayList<>();
    private String serverId = Integer.toHexString(new Random().nextInt());
    private String status;
    private Executor executor = Executors.newSingleThreadExecutor();
    private ZooKeeper zooKeeper;
    private String name;
    private Watcher newTaskWatcher = event -> {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            assert ("/assign/worker-" + serverId).equals(event.getPath());
            getTasks();
        }
    };
    private AsyncCallback.DataCallback taskDataCallBack = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                log.info("task finished");
                break;
            default:
                log.error("task received failure", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };
    private AsyncCallback.Children2Callback taskGetChildrenCallBack = (rc, path, ctx, children, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if (children != null) {
                    executor.execute(new TaskRunner().init(children, taskDataCallBack));
                }
        }
    };
    private AsyncCallback.StringCallback assignNodeCreateCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignNode();
                break;
            case OK:
                log.info("create assign task");
                break;
            case NODEEXISTS:
                log.error("no node for {}", path);
                break;
            default:
                log.error("assign task error", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    public Worker(String name, ZooKeeper zooKeeper) {
        this.name = name;
        this.zooKeeper = zooKeeper;
        WorkersCenter.registerWorker(this);
        createAssignNode();
    }


    private void createAssignNode() {
        zooKeeper.create("/assign/worker-" + serverId, new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignNodeCreateCallback, null);
    }

    private void getTasks() {
        zooKeeper.getChildren("/assign/worker-" + serverId, newTaskWatcher, taskGetChildrenCallBack, null);
    }

    public void register() {
        zooKeeper.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallBack(), null);
    }

    private AsyncCallback.StringCallback createWorkerCallBack() {
        return (rc, path, ctx, name) -> {
            log.info("rc:{},path:{},ctx:{},name:{}", rc, path, ctx, name);
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    log.info("Registered successfully:{}", serverId);
                    getTasks();
                    break;
                case NODEEXISTS:
                    log.warn("Already registered:{}", serverId);
                    break;
                default:
                    log.error("Something went wrong:{}", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        };
    }

    private AsyncCallback.StatCallback statusUpdateCallBack() {
        return (rc, path, ctx, stat) -> {
            if (KeeperException.Code.get(rc) == KeeperException.Code.CONNECTIONLOSS) {
                updateStatus((String) ctx);
            }
        };
    }

    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            zooKeeper.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallBack(), status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private class TaskRunner implements Runnable {
        List<String> children;
        AsyncCallback.DataCallback cb;

        Runnable init(List<String> children, AsyncCallback.DataCallback cb) {
            this.children = children;
            this.cb = cb;
            return this;
        }

        @Override
        public void run() {
            log.info("Looping into tasks");
            synchronized (onGoingTasks) {
                for (String task : children) {
                    if (!onGoingTasks.contains(task)) {
                        log.info("New Task:{}", task);
                        zooKeeper.getData("/assign/worker-" + serverId + "/" + task, false, cb, task);
                        onGoingTasks.add(task);
                    }
                }
            }
        }
    }
}

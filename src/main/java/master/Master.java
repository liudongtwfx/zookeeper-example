package master;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event;

import java.util.List;
import java.util.Random;

@Slf4j
public class Master {
    private String serverId = Integer.toHexString(new Random().nextInt());

    private boolean isLeader = false;
    private List<String> workers;

    private ZooKeeper zooKeeper;
    private AsyncCallback.Children2Callback workersGetChildrenCallBack = (rc, path, ctx, children, stat) -> {
        children.forEach(child -> log.info("child:{}", child));
        workers = children;
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                log.info("successfully got a list of workers: {} workers", children.size());
                break;
            default:
                log.error("getChildren failed", KeeperException.create(Code.get(rc), path));
        }
    };
    private Watcher workersChangedWatcher = event -> {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/workers".equals(event.getPath());
            getWorkers();
        }
    };
    private AsyncCallback.StringCallback assignTaskCallBack = (rc, path, ctx, name) -> {
        log.info("assign task callback rc:{}", rc);
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                break;
            case OK:
                log.info("Task assigned correctly: {}", name);
                deleteTask(name.substring(name.lastIndexOf("/") + 1));
                break;
            case NODEEXISTS:
                log.warn("Task already assigned");
                break;
            default:
                log.error("Error when trying to assign task", KeeperException.create(Code.get(rc), path));
        }
    };
    private AsyncCallback.DataCallback taskDateCallBack = (rc, path, ctx, data, stat) -> {
        log.info("rc:{}", rc);
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                break;
            case OK:
                if (workers != null && !workers.isEmpty()) {
                    int worker = new Random().nextInt(workers.size());
                    String designateWorker = workers.get(worker);
                    String assignmentPath = "/assign/" + designateWorker + "/" + ctx;
                    createAssignment(assignmentPath, data);
                }
                break;
            default:
                log.error("Error when trying to get task data", KeeperException.create(Code.get(rc), path));
        }
    };
    private AsyncCallback.Children2Callback taskGetChildrenCallBack = (rc, path, ctx, children, stat) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if (children != null) {
                    assignTasks(children);
                }
                break;
            default:
                log.error("getChildren failed.", KeeperException.create(Code.get(rc), path));
        }
    };
    private Watcher tasksChangedWatcher = event -> {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/tasks".equals(event.getPath());
            getTasks();
        }
    };
    private AsyncCallback.StatCallback masterExistsCallBack = (rc, path, ctx, stat) -> {
        log.info("path:{}", path);
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                if (stat == null) {
                    runForMaster();
                }
                break;
            default:
                checkMaster();
        }
    };
    private Watcher masterExistsWatcher = event -> {
        log.info("event:{}", event);
        if (event.getType() == Event.EventType.NodeDeleted) {
            assert "/master".equals(event.getPath());
            runForMaster();
        }
    };

    public Master(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        masterExists();
    }

    private void deleteTask(String taskName) {
        try {
            zooKeeper.delete("/tasks/" + taskName, 0);
        } catch (Exception e) {
            log.error("exception", e);
        }
    }

    public void createAssignment(String path, byte[] data) {
        assert path.startsWith("/assign/");
        System.out.println("create assignment:" + path);
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallBack, data);
    }

    private void getTasks() {
        zooKeeper.getChildren("/tasks",
                tasksChangedWatcher, taskGetChildrenCallBack, null);
    }

    public void runForMaster() {
        zooKeeper.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterStringCallback(), null);
        isLeader = true;
    }

    public void checkMaster() {
        zooKeeper.getData("/master", MasterEventWatcher.MASTER_EVENT_WATCHER, dataCallback(), null);
    }

    private AsyncCallback.StringCallback masterStringCallback() {
        return (rc, path, ctx, name) -> {
            log.info("rc:{},path:{},ctx:{},name:{}", rc, path, ctx, name);
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    getWorkers();
                    getTasks();
                    return;
                case NODEEXISTS:
                    masterExists();
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") + " the leader");
        };
    }

    private AsyncCallback.DataCallback dataCallback() {
        return (rc, path, ctx, data, stat) -> {
            if (data != null) {
                log.info("data is:{}", new String(data));
                isLeader = new String(data).equals(serverId);
            }
        };
    }

    public boolean isLeader() {
        return isLeader;
    }

    private void masterExists() {
        zooKeeper.exists("/master", masterExistsWatcher, masterExistsCallBack, null);
    }

    public String getServerId() {
        return serverId;
    }

    private void getWorkers() {
        zooKeeper.getChildren("/workers", workersChangedWatcher, workersGetChildrenCallBack, null);
    }

    private void assignTasks(List<String> tasks) {
        tasks.forEach(this::getTaskData);
    }

    private void getTaskData(String task) {
        zooKeeper.getData("/tasks/" + task,
                false, taskDateCallBack, task);
    }

    private static class MasterEventWatcher implements Watcher {
        private static final MasterEventWatcher MASTER_EVENT_WATCHER = new MasterEventWatcher();

        private MasterEventWatcher() {
        }

        @Override
        public void process(WatchedEvent event) {
            log.info("{}", event);
            if (event.getType() == Event.EventType.None) {
                log.info("master changed");
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                String path = event.getPath();
                System.out.println(path);
            }
        }
    }
}

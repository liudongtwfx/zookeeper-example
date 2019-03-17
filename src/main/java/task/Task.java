package task;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class Task {
    private ZooKeeper zooKeeper;
    private Map<String, Object> ctxMap = new ConcurrentHashMap<>();
    private AsyncCallback.StatCallback existsCallBack = (rc, path, ctx, stat) -> {

    };
    private AsyncCallback.DataCallback getDataCallBack = (rc, path, ctx, data, stat) -> {

    };

    private Watcher statusWatcher = event -> {
        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
            assert event.getPath().contains("/status/task-");
            zooKeeper.getData(event.getPath(), false, getDataCallBack, ctxMap.get(event.getPath()));
        }
    };
    private AsyncCallback.StringCallback createTaskCallBack = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                submitTask(((TaskObject) ctx).getTask(), (TaskObject) ctx);
                break;
            case OK:
                log.info("My Created Task Name: {}", name);
                ((TaskObject) ctx).setTaskName(name);
                watchStatus("/status/" + name.replace("/tasks/", ""), ctx);
        }
    };

    public Task(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    private void submitTask(String task, TaskObject taskObject) {
        taskObject.setTask(task);
        zooKeeper.create("/tasks/task-",
                task.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallBack, taskObject);
    }

    private void watchStatus(String path, Object ctx) {
        ctxMap.put(path, ctx);
        zooKeeper.exists(path, statusWatcher, existsCallBack, ctx);
    }
}

package client;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class Client {
    private ZooKeeper zooKeeper;

    public Client(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void queueCommand(String command) {
        try {
            String path = zooKeeper.create("/tasks/task-",
                    command.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            log.info("path:{}", path);
        } catch (KeeperException e) {
            queueCommand(command);
        } catch (InterruptedException e) {
            log.error("exception", e);
        }
    }
}

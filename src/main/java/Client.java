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

    public String queueCommand(String command) {
        while (true) {
            try {
                return zooKeeper.create("/tasks/task-",
                        command.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                log.error("exception", e);
            }
        }
    }
}

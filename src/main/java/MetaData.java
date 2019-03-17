import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;

@Slf4j
public class MetaData {
    private ZooKeeper zooKeeper;

    public MetaData(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    private void createParent(String path, byte[] data) {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallBack(), data);
    }

    private AsyncCallback.StringCallback createParentCallBack() {
        return (rc, path, ctx, name) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    log.info("Parent created");
                    break;
                case NODEEXISTS:
                    log.warn("Parent already registered:{}", path);
                    break;
                default:
                    log.error("Something went wrong: {}", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        };
    }
}

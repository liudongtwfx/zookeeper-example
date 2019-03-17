import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

@Slf4j
public class AdminClient implements Watcher {
    private ZooKeeper zooKeeper;

    public AdminClient(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        log.info("{}", event);
    }

    public void listState() throws Exception {
        try {
            Stat stat = new Stat();
            byte[] masterData = zooKeeper.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            log.info("master.Master:{}, since {}", new String(masterData), startDate);
        } catch (Exception e) {
            log.error("exception", e);
        }
        System.out.println("Workers:");
        for (String w : zooKeeper.getChildren("/workers", false)) {
            byte[] data = zooKeeper.getData("/workers/" + w, false, null);
            log.info("\t{}:{}", w, new String(data));
        }
        System.out.println("Tasks:");
        for (String t : zooKeeper.getChildren("/tasks", false)) {
            log.info("\t{}", t);
        }
    }
}

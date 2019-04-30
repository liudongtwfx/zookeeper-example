import client.Client;
import master.Master;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import worker.Worker;

/**
 * @author liudong17
 */
public class Main {
    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183/", 15000, new ZookeeperWatcher());
        zooKeeper.addAuthInfo(new DigestAuthenticationProvider().getScheme(), "CREATE".getBytes());
        Master master = new Master(zooKeeper);
        master.runForMaster();
        System.out.println(master.getServerId());

        MetaData metaData = new MetaData(zooKeeper);
        metaData.bootstrap();

        Worker worker = new Worker("MyWork", zooKeeper);
        worker.register();

        Worker two = new Worker("MyWorkTwo", zooKeeper);
        two.register();

        Client client = new Client(zooKeeper);
        client.queueCommand("hello world");
        client.queueCommand("mvn clean");
        client.queueCommand("Hi");
        client.queueCommand("mvn spring-boot:run");

        Master anotherMaster = new Master(zooKeeper);
        anotherMaster.runForMaster();
        System.out.println(anotherMaster.getServerId());

        AdminClient adminClient = new AdminClient(zooKeeper);
        adminClient.listState();
        Thread.sleep(10 * 1000L);
        zooKeeper.close();
        System.exit(1);
    }
}

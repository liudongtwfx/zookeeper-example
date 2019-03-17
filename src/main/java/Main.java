import master.Master;
import org.apache.zookeeper.ZooKeeper;
import worker.Worker;

public class Main {
    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("localhost", 15000, new ZookeeperWatcher());
        Master master = new Master(zooKeeper);
        master.runForMaster();
        System.out.println(master.getServerId());

        MetaData metaData = new MetaData(zooKeeper);
        metaData.bootstrap();

        Worker worker = new Worker("MyWork", zooKeeper);
        worker.register();

        Worker two = new Worker("MyWorkTwo", zooKeeper);
        worker.register();

        Client client = new Client(zooKeeper);
        client.queueCommand("hello world");

        Master anotherMaster = new Master(zooKeeper);
        anotherMaster.runForMaster();
        System.out.println(anotherMaster.getServerId());


        AdminClient adminClient = new AdminClient(zooKeeper);
        adminClient.listState();
        Thread.sleep(1000 * 1000);
        zooKeeper.close();
    }
}

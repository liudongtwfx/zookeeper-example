package server;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerOneTest {
    public static void main(String[] args) {
        QuorumPeerMainTest one = new QuorumPeerMainTest(new String[]{"/Users/liudong/program_files/zookeeper-3.4.13/conf/zoo.cfg"});
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(one).start();
    }
}

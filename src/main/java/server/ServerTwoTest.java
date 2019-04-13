package server;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerTwoTest {
    public static void main(String[] args) {
        QuorumPeerMainTest two = new QuorumPeerMainTest(new String[]{"/Users/liudong/program_files/zookeeper-3.4.13/conf/zoo1.cfg"});
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(two).start();
    }
}

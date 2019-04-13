package server;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerThreeTest {
    public static void main(String[] args) {
        QuorumPeerMainTest three = new QuorumPeerMainTest(new String[]{"/Users/liudong/program_files/zookeeper-3.4.13/conf/zoo2.cfg"});
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(three).start();
    }
}

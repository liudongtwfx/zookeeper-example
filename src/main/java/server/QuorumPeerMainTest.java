package server;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumStats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class QuorumPeerMainTest extends QuorumPeerMain implements Runnable {
    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";
    private String[] args;

    public QuorumPeerMainTest(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        throw new RuntimeException("Please config server properties");
    }

    @Override
    public void run() {
        runForArgs();
    }

    private void runForArgs() {
        try {
            executorService.scheduleAtFixedRate(new ReportRunner(), 5, 20, TimeUnit.SECONDS);
            super.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            log.error("Invalid arguments, exiting abnormally", e);
            log.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (QuorumPeerConfig.ConfigException e) {
            log.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
        } catch (Exception e) {
            log.error("Unexpected exception, exiting abnormally", e);
        }
        log.info("Exiting normally");
    }

    public void printServerState() {
        switch (quorumPeer.getServerState()) {
            case QuorumStats.Provider.LEADING_STATE:
                System.out.println(quorumPeer.getMyid() + ": is leader");
                break;
            case QuorumStats.Provider.FOLLOWING_STATE:
                System.out.println(quorumPeer.getMyid() + ":" + quorumPeer.follower);
                break;
            default:
                System.out.println(quorumPeer.getServerState());
        }
    }

    class ReportRunner implements Runnable {
        @Override
        public void run() {
            printServerState();
        }
    }
}

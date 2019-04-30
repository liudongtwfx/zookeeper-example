package server;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumStats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.System.err;
import static java.lang.System.out;

/**
 * @author liudong
 */
@Slf4j
public class QuorumPeerMainTest extends QuorumPeerMain implements Runnable {
    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

    private static final String USAGE = "Usage: QuorumPeerMain config file";
    private String[] args;

    QuorumPeerMainTest(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        throw new IllegalStateException("Please config server properties");
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
            err.println(USAGE);
            System.exit(2);
        } catch (QuorumPeerConfig.ConfigException e) {
            log.error("Invalid config, exiting abnormally", e);
            err.println("Invalid config, exiting abnormally");
        } catch (Exception e) {
            log.error("Unexpected exception, exiting abnormally", e);
        }
        log.info("Exiting normally");
    }

    private void printServerState() {
        switch (quorumPeer.getServerState()) {
            case QuorumStats.Provider.LEADING_STATE:
                out.println(quorumPeer.getMyid() + ": is leader");
                break;
            case QuorumStats.Provider.FOLLOWING_STATE:
                out.println(quorumPeer.getMyid() + ":" + quorumPeer.follower);
                break;
            default:
                out.println(quorumPeer.getServerState());
        }
    }

    class ReportRunner implements Runnable {
        @Override
        public void run() {
            printServerState();
        }
    }
}

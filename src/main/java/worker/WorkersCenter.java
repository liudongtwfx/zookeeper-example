package worker;

import java.util.HashSet;
import java.util.Set;

public class WorkersCenter {
    private static Set<Worker> WORKERS = new HashSet<>();

    public static void registerWorker(Worker worker) {
        WORKERS.add(worker);
    }

    public static Set<Worker> getWORKERS() {
        return WORKERS;
    }
}

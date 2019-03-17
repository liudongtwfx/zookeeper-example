package callbacks;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

@Slf4j
public class TaskDataCallBack implements AsyncCallback.DataCallback {
    private static final TaskDataCallBack INSTANCE = new TaskDataCallBack();

    private TaskDataCallBack() {
    }


    public static TaskDataCallBack getINSTANCE() {
        return INSTANCE;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

    }
}

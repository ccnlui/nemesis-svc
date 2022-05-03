package nemesis.svc;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;

final class Config {
    static final String    queueBasePath;
    static final RollCycle roleCycle;

    static {
        String p;

        p = System.getenv("QUEUE_BASEPATH");
        if (p != null)
            queueBasePath = p;
        else
            queueBasePath = OS.getTarget() + "/queue";
        
        roleCycle   = RollCycles.MINUTELY;

        System.out.println("---------------------------------------------------");
        System.out.println("QUEUE_BASEPATH: " + queueBasePath);
        System.out.println("---------------------------------------------------");
    }

    // Hide constructor because this is a "static" class.
    private Config() {
    }
}

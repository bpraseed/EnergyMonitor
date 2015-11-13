import java.util.Random;

/**
 * Created by praseed on 11/12/15.
 */
public class EnegryMonitorDataGen {

    protected int numOfUsers;

    public EnegryMonitorDataGen(int numOfUsers) {
        this.numOfUsers = numOfUsers;
    }

    EnegryMonitorRecord next() {
        Random random= new Random();
        EnegryMonitorRecord energyMonitorRecord = new EnegryMonitorRecord(random.nextInt(numOfUsers), random.nextLong(),System.currentTimeMillis());
        return energyMonitorRecord;
    }
}

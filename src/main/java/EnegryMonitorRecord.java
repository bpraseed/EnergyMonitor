/**
 * Created by praseed on 11/12/15.
 */
public class EnegryMonitorRecord {
    int userId;
    long usedAmount;
    long time;

    public EnegryMonitorRecord(int userId, long userAmt, long time) {
        this.userId = userId;
        this.usedAmount = userAmt;
        this.time = time;
    }
}

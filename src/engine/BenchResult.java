package engine;

public class BenchResult {

    public enum ExecStatus {
        OK,
        KO
    }

    private ExecStatus status;
    private Exception ex;

    public ExecStatus getStatus() {
        return status;
    }

    public void setStatus(ExecStatus status) {
        this.status = status;
    }

    public Exception getEx() {
        return ex;
    }

    public void setEx(Exception ex) {
        this.ex = ex;
    }

}

package engine.dto;

import lombok.Data;

@Data
public class BenchResult {

    public enum ExecStatus {
        OK,
        KO
    }

    private ExecStatus status;
    private Exception ex;

}

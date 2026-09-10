import java.io.Serializable;

public class VoteResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int term;
    private final boolean voteGranted;

    public VoteResponse(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "VoteResponse{term=" + term + ", voteGranted=" + voteGranted + "}";
    }
}

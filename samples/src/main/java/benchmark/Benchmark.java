package benchmark;

import java.util.List;

public interface Benchmark
{
    void initialize(List<String> requests);
    void replaceNodes();
    void addNodes();
    void deleteNodes();
    boolean isSameNode(String key);
    String getBenchmarkName();
}

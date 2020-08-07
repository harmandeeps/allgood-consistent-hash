package benchmark;

import java.util.Collection;
import java.util.List;

public interface Benchmark
{
    void initialize(List<String> requests);
    void locateKeys(List<String> requests);
    void replaceNodes();
    void addNodes();
    void deleteNodes();
    boolean isSameNode(String key);
    int getRingSize();
    String getBenchmarkName();
    Collection<Integer> getNodeLoadList();
}

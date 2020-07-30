package benchmark;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.Random;

public class AdvanceConsistentHash
    extends BasicConsistentHash
{

    public AdvanceConsistentHash(int nodesCount, int nodesDelta)
    {
        super(nodesCount, nodesDelta);
    }

    @Override
    public void deleteNodes()
    {
        long previousSize = ring.stream().filter(simpleNode -> simpleNode.getNodeState().equals(NodeState.RUNNING)).count();
        long prevStopped = ring.stream().filter(simpleNode -> simpleNode.getNodeState().equals(NodeState.STOPPED)).count();
        // remove some nodes from the ring
        Random rand = new Random();
        for (int i = 0; i < nodesDelta; i++) {
            int nodeIndex = rand.nextInt(ring.size() - 1);
            while(ring.get(nodeIndex).getNodeState() != NodeState.RUNNING)
            {
                nodeIndex = rand.nextInt(ring.size() - 1);
            }
            SimpleNode changeNode = ring.remove(nodeIndex);
            changeNode.setNodeState(NodeState.STOPPED);
            ring.add(nodeIndex, changeNode);
        }
        //System.out.println("delete nodes: " + ring.size());
        assert ring.stream().filter(simpleNode -> simpleNode.getNodeState().equals(NodeState.RUNNING)).count()
                == (previousSize-nodesDelta);
        assert ring.stream().filter(simpleNode -> simpleNode.getNodeState().equals(NodeState.STOPPED)).count()-prevStopped
                == (nodesDelta);
    }

    public int getNodeIndex(String key)
    {
        HashFunction hf = Hashing.md5();
        hf = Hashing.hmacMd5(hf.hashString(key, Charsets.UTF_8).asBytes());
        HashCode hc = hf.hashString(key, Charsets.UTF_8);
        int nodeIndex = Hashing.consistentHash(hc, ring.size());

        if (hc.asInt() % 2 == 0) {
            while(!ring.get(nodeIndex).getNodeState().equals(NodeState.RUNNING)) {
                //System.out.println(" increasing " + ring.get(nodeIndex).getName());
                nodeIndex = (nodeIndex+1)%ring.size();
            }
        }
        else {
            while(!ring.get(nodeIndex).getNodeState().equals(NodeState.RUNNING)) {
                //System.out.println(" decreasing " + ring.get(nodeIndex).getName());
                if (nodeIndex == 0) {
                    nodeIndex = ring.size();
                }
                nodeIndex = (nodeIndex-1)%ring.size();
            }
        }
        return nodeIndex;
    }

    @Override
    public int getRingSize()
    {
        return ring.size();
        //return (int) ring.stream().filter(simpleNode -> simpleNode.getNodeState().equals(NodeState.RUNNING)).count();
    }

    @Override
    public String getBenchmarkName()
    {
        return "Advance Consistent Hash";
    }
}

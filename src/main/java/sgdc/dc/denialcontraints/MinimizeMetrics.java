// 放在 de.hpi.naumann.dc.denialcontraints 或 cover 包均可
package sgdc.dc.denialcontraints;

import org.slf4j.Logger;

public class MinimizeMetrics {
    public long nInputDC;             // 输入 DC 数
    public long nKeptAfterClosure;    // construct() 成功并进入闭包 map 的 DC 数
    public long nInvTried;            // 尝试构造 inv 的次数
    public long nInvKept;             // inv construct() 成功的次数

    public long timeBuildClosureNs;   // 并行阶段：为 DC 构造 closure 的耗时
    public long timeBuildInvNs;       // 并行阶段：为 inv 构造 closure 的耗时
    public long timeSortNs;           // 排序耗时
    public long timeFinalTrieNs;      // 串行 Trie 过滤耗时

    public long trieContainsCalls;    // containsSubset 调用次数
    public long trieAdds;             // add 调用次数（候选添加到 Trie）

    public void log(Logger log) {
        double ms = 1e-6;
        log.info(String.format(
                "Minimize metrics | input=%d | kept=%d | invTried=%d | invKept=%d | buildClosure=%.3fms | buildInv=%.3fms | sort=%.3fms | finalTrie=%.3fms | trieContains=%d | trieAdds=%d",
                nInputDC, nKeptAfterClosure, nInvTried, nInvKept,
                timeBuildClosureNs*ms, timeBuildInvNs*ms, timeSortNs*ms, timeFinalTrieNs*ms,
                trieContainsCalls, trieAdds
        ));
    }
}

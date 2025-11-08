package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.util.Iterator;
import java.util.Optional;

/**
 * An inner node or a leaf node. See InnerNode and LeafNode for more
 * information.
 */
abstract class BPlusNode {
    // Core API ////////////////////////////////////////////////////////////////
    /**
     * n.get(k) returns the leaf node on which k may reside when queried from n.
     * For example, consider the following B+ tree (for brevity, only keys are
     * shown; record ids are omitted).
     *
     * n.get(k) 返回可能包含键 k 的叶节点，从节点 n 查询。
     * 例如，考虑以下 B+ 树（为简洁起见，仅显示键；记录 ID 已省略）
     *
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * inner.get(x) should return
     *
     *   - leaf0 when x < 10,
     *   - leaf1 when 10 <= x < 20, and
     *   - leaf2 when x >= 20.
     *
     * Note that inner.get(4) would return leaf0 even though leaf0 doesn't
     * actually contain 4.
     *
     * inner.get(x) 应返回：
     *
     * 当 x < 10 时，返回 leaf0；
     * 当 10 <= x < 20 时，返回 leaf1；
     * 当 x >= 20 时，返回 leaf2。
     *
     *
     * 注意，inner.get(4) 将返回 leaf0，即使 leaf0 实际上不包含键 4。
     */
    public abstract LeafNode get(DataBox key);

    /**
     * n.getLeftmostLeaf() returns the leftmost leaf in the subtree rooted by n.
     * In the example above, inner.getLeftmostLeaf() would return leaf0, and
     * leaf1.getLeftmostLeaf() would return leaf1.
     *
     * n.getLeftmostLeaf() 返回以节点 n 为根的子树中最左边的叶节点。
     * 在上面的例子中，inner.getLeftmostLeaf() 将返回 leaf0，而 leaf1.getLeftmostLeaf() 将返回 leaf1。
     */
    public abstract LeafNode getLeftmostLeaf();

    /**
     * n.put(k, r) inserts the pair (k, r) into the subtree rooted by n. There
     * n.put(k, r) 将键值对 (k, r) 插入到以节点 n 为根的子树中。需要考虑以下两种情况：
     * are two cases to consider:
     *
     *   Case 1: If inserting the pair (k, r) does NOT cause n to overflow, then
     *           Optional.empty() is returned.
     *   情况 1：如果插入键值对 (k, r) 不会导致节点 n 溢出，则返回 Optional.empty()。
     *
     *   Case 2: If inserting the pair (k, r) does cause the node n to overflow,
     *           then n is split into a left and right node (described more
     *           below) and a pair (split_key, right_node_page_num) is returned
     *           where right_node_page_num is the page number of the newly
     *           created right node, and the value of split_key depends on
     *           whether n is an inner node or a leaf node (described more below).
     *情况 2：如果插入键值对 (k, r) 导致节点 n 溢出，则将节点 n 分裂为左右两个节点（具体描述见下文），
     * 并返回一个键值对 (split_key, right_node_page_num)，
     * 其中 right_node_page_num 是新创建的右节点的页面编号，
     * split_key 的值取决于 n 是内部节点还是叶节点（具体描述见下文）。
     *
     * Now we explain how to split nodes and which split keys to return. Let's
     * take a look at an example. Consider inserting the key 4 into the example
     * tree above. No nodes overflow (i.e. we always hit case 1). The tree then
     * looks like this:
     * 现在我们解释如何分裂节点以及返回哪个分裂键。我们来看一个例子。
     * 考虑将键 4 插入到上述示例树中。没有任何节点溢出（即总是命中情况 1）。树看起来如下：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |  4 |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Now let's insert key 5 into the tree. Now, leaf0 overflows and creates a
     * new right sibling leaf3. d entries remain in the left node; d + 1 entries
     * are moved to the right node. DO NOT REDISTRIBUTE ENTRIES ANY OTHER WAY. In
     * our example, leaf0 and leaf3 would look like this:
     *
     * 现在将键 5 插入到树中。此时，leaf0 溢出并创建一个新的右兄弟节点 leaf3。左节点保留 d 个条目；d + 1 个条目被移动到右节点。
     * 不要以其他方式重新分配条目。在我们的例子中，leaf0 和 leaf3 看起来如下：
     *
     *   +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |    |    |->|  3 |  4 |  5 |    |
     *   +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf3
     *
     * When a leaf splits, it returns the first entry in the right node as the
     * split key. In this example, 3 is the split key. After leaf0 splits, inner
     * inserts the new key and child pointer into itself and hits case 1 (i.e. it
     * does not overflow). The tree looks like this:
     * 当叶节点分裂时，返回右节点中的第一个条目作为分裂键。在这个例子中，分裂键是 3。
     * 在 leaf0 分裂后，inner 节点将新键和子节点指针插入自身，并命中情况 1（即不溢出）。树看起来如下：
     *
     *                          inner
     *                          +--+--+--+--+
     *                          | 3|10|20|  |
     *                          +--+--+--+--+
     *                         /   |  |   \
     *                 _______/    |  |    \_________
     *                /            |   \             \
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   | 1| 2|  |  |->| 3| 4| 5|  |->|11|12|13|  |->|21|22|23|  |
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   leaf0          leaf3          leaf1          leaf2
     *
     * When an inner node splits, the first d entries are kept in the left node
     * and the last d entries are moved to the right node. The middle entry is
     * moved (not copied) up as the split key. For example, we would split the
     * following order 2 inner node
     *
     * 当内部节点分裂时，左节点保留前 d 个条目，最后 d 个条目移动到右节点。
     * 中间条目被**移动（而不是复制）**作为分裂键。例如，对于以下阶数为 2 的内部节点：
     *
     *   +---+---+---+---+
     *   | 1 | 2 | 3 | 4 | 5
     *   +---+---+---+---+
     *
     * into the following two inner nodes
     *
     *   +---+---+---+---+  +---+---+---+---+
     *   | 1 | 2 |   |   |  | 4 | 5 |   |   |
     *   +---+---+---+---+  +---+---+---+---+
     *
     * with a split key of 3.
     *
     * DO NOT redistribute entries in any other way besides what we have
     * described. For example, do not move entries between nodes to avoid
     * splitting.
     * 不要以除此之外的其他方式重新分配条目。例如，不要为了避免分裂而在节点之间移动条目。
     *
     * Our B+ trees do not support duplicate entries with the same key. If a
     * duplicate key is inserted into a leaf node, the tree is left unchanged
     * and a BPlusTreeException is raised.
     * 我们的 B+ 树不支持具有相同键的重复条目。如果向叶节点插入重复键，树保持不变，并抛出 BPlusTreeException 异常。
     */
    public abstract Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid);

    /**
     * n.bulkLoad(data, fillFactor) bulk loads pairs of (k, r) from data into
     * the tree with the given fill factor.
     *  n.bulkLoad(data, fillFactor) 将 data 中的键值对 (k, r)
     *  批量加载到以节点 n 为根的 B+ 树中，并使用给定的填充因子 fillFactor。
     *
     * This method is very similar to n.put, with a couple of differences:
     *  该方法与 n.put 非常相似，但有以下几点不同：
     *
     * 1. Leaf nodes do not fill up to 2*d+1 and split, but rather, fill up to
     * be 1 record more than fillFactor full, then "splits" by creating a right
     * sibling that contains just one record (leaving the original node with
     * the desired fill factor).
     *
     * 叶节点不会填充到 2*d+1 条记录后分裂，而是填充到比 fillFactor 多一条记录，
     * 然后通过创建一个只包含一条记录的右兄弟节点进行“分裂”（原始节点保持所需的填充因子）。
     *
     * 2. Inner nodes should repeatedly try to bulk load the rightmost child
     * until either the inner node is full (in which case it should split)
     * or there is no more data.
     * 内部节点应反复尝试将数据批量加载到最右边的子节点，直到内部节点已满（此时应分裂）或没有更多数据为止。
     *
     * fillFactor should ONLY be used for determining how full leaf nodes are
     * (not inner nodes), and calculations should round up, i.e. with d=5
     * and fillFactor=0.75, leaf nodes should be 8/10 full.
     *  fillFactor 仅用于确定叶节点的填充程度（不用于内部节点），且计算应向上取整。
     * 例如，当 d=5 且 fillFactor=0.75 时，叶节点应填充到 8/10 的容量。
     *
     * You can assume that 0 < fillFactor <= 1 for testing purposes, and that
     * a fill factor outside of that range will result in undefined behavior
     * (you're free to handle those cases however you like).
     *
     * 为测试目的，可以假设 0 < fillFactor <= 1，且填充因子超出此范围将导致未定义行为（你可以自由选择如何处理这些情况）。
     */
    public abstract Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor);

    /**
     * n.remove(k) removes the key k and its corresponding record id from the
     * subtree rooted by n, or does nothing if the key k is not in the subtree.
     * REMOVE SHOULD NOT REBALANCE THE TREE. Simply delete the key and
     * corresponding record id. For example, running inner.remove(2) on the
     * example tree above would produce the following tree.
     * n.remove(k) 从以节点 n 为根的子树中删除键 k 及其对应的记录 ID，
     *  如果键 k 不在该子树中，则不执行任何操作。
     *  **删除操作不应重新平衡树**。只需简单地删除该键及其对应的记录 ID。
     *  例如，对上面的示例树执行 inner.remove(2)，将得到以下树结构：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  3 |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Running inner.remove(1) on this tree would produce the following tree:
     * 再对该树执行 inner.remove(1)，将得到以下树结构：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  3 |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Running inner.remove(3) would then produce the following tree:
     * 继续执行 inner.remove(3)，将得到以下树结构：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |    |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Again, do NOT rebalance the tree.
     * 再次强调：不要重新平衡树**。
     */
    public abstract void remove(DataBox key);

    // Helpers /////////////////////////////////////////////////////////////////
    /** Get the page on which this node is persisted. */
    abstract Page getPage();

    // Pretty Printing /////////////////////////////////////////////////////////
    /**
     * S-expressions (or sexps) are a compact way of encoding nested tree-like
     * structures (sort of like how JSON is a way of encoding nested dictionaries
     * and lists). n.toSexp() returns an sexp encoding of the subtree rooted by
     * n. For example, the following tree:
     *
     *                      +---+
     *                      | 3 |
     *                      +---+
     *                     /     \
     *   +---------+---------+  +---------+---------+
     *   | 1:(1 1) | 2:(2 2) |  | 3:(3 3) | 4:(4 4) |
     *   +---------+---------+  +---------+---------+
     *
     * has the following sexp
     *
     *   (((1 (1 1)) (2 (2 2))) 3 ((3 (3 3)) (4 (4 4))))
     *
     * Here, (1 (1 1)) represents the mapping from key 1 to record id (1, 1).
     */
    public abstract String toSexp();

    /**
     * n.toDot() returns a fragment of a DOT file that draws the subtree rooted
     * at n.
     */
    public abstract String toDot();

    // Serialization ///////////////////////////////////////////////////////////
    /** n.toBytes() serializes n. */
    public abstract byte[] toBytes();

    /**
     * BPlusNode.fromBytes(m, p) loads a BPlusNode from page `pageNum`.
     */
    public static BPlusNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                      LockContext treeContext, long pageNum) {
        Page p = bufferManager.fetchPage(treeContext, pageNum);
        try {
            Buffer buf = p.getBuffer();
            byte b = buf.get();
            if (b == 1) {
                return LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else if (b == 0) {
                return InnerNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else {
                String msg = String.format("Unexpected byte %b.", b);
                throw new IllegalArgumentException(msg);
            }
        } finally {
            p.unpin();
        }
    }
}

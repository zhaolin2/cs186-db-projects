package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A inner node of a B+ tree. Every inner node in a B+ tree of order d stores
 * between d and 2d keys. An inner node with n keys stores n + 1 "pointers" to
 * children nodes (where a pointer is just a page number). Moreover, every
 * inner node is serialized and persisted on a single page; see toBytes and
 * fromBytes for details on how an inner node is serialized. For example, here
 * is an illustration of an order 2 inner node:
 *
 *     +----+----+----+----+
 *     | 10 | 20 | 30 |    |
 *     +----+----+----+----+
 *    /     |    |     \
 */
class InnerNode extends BPlusNode {
    // Metadata about the B+ tree that this node belongs to.
    private BPlusTreeMetadata metadata;

    // Buffer manager
    private BufferManager bufferManager;

    // Lock context of the B+ tree
    private LockContext treeContext;

    // The page on which this leaf is serialized.
    private Page page;

    // The keys and child pointers of this inner node. See the comment above
    // LeafNode.keys and LeafNode.rids in LeafNode.java for a warning on the
    // difference between the keys and children here versus the keys and children
    // stored on disk. `keys` is always stored in ascending order.
    // 此内部节点的键和子节点指针。
// 有关此处 keys 和 children 与磁盘上存储的键和子节点之间的区别，
// 请参阅 LeafNode.java 中 LeafNode.keys 和 LeafNode.rids 上方的注释警告。
// `keys` 始终按升序存储。
    private List<DataBox> keys;
    private List<Long> children;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Construct a brand new inner node.
     */
    InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
              List<Long> children, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
             keys, children, treeContext);
    }

    /**
     * Construct an inner node that is persisted to page `page`.
     */
    private InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                      List<DataBox> keys, List<Long> children, LockContext treeContext) {
        try {
            assert (keys.size() <= 2 * metadata.getOrder());
            assert (keys.size() + 1 == children.size());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.children = new ArrayList<>(children);
            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(DataBox key) {
        // TODO(proj2): implement

//        for (int i = 0; i < this.keys.size(); i++) {
//            DataBox dataBox = this.keys.get(i);
//
//            //小于当前节点 则返回左指针
//            if (key.compareTo(dataBox)<0){
//                BPlusNode child = this.getChild(i);
//                if (child instanceof LeafNode) {
//                    return (LeafNode) child;
//                }else {
//                    return child.get(key);
//                }
//            }
//        }

        DataBox firstKey = this.keys.get(0);
        if (key.compareTo(firstKey)<0){
            return getChild(0).get(key);
        }

        int i = 0;
        int j = i+1;

        int size = this.keys.size();

        for (; i < size && j < size; i++,j++) {
            DataBox leftNode = this.keys.get(i);
            DataBox rightNode = this.keys.get(j);
            if (key.compareTo(leftNode)>=0 && key.compareTo(rightNode)<0){
                return getChild(j).get(key);
            }
        }



        return this.getChild(size).get(key);



        //需要获取最右边的节点
//        return this.getChild(this.children.size() - 1).get(key);
    }

    public BPlusNode getOneLevel(DataBox key) {
        // TODO(proj2): implement
//
//        DataBox firstKey = this.keys.get(0);
//        if (key.compareTo(firstKey)<0){
//            return getChild(0);
//        }
//
//        int i = 0;
//        int j = i+1;
//
//        int size = this.keys.size();
//
//        for (; i < size && j < size; i++,j++) {
//            DataBox leftNode = this.keys.get(i);
//            DataBox rightNode = this.keys.get(j);
//            if (key.compareTo(leftNode)>=0 && key.compareTo(rightNode)<0){
//                return getChild(j);
//            }
//        }
//
//
//
//        return this.getChild(size);

        int idx = 0;
        for (; idx < keys.size(); idx++) {
            if (key.compareTo(keys.get(idx)) < 0) break;
        }

        return getChild(idx);
    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf() {
        assert(children.size() > 0);
        // TODO(proj2): implement

        LeafNode leafNode = null;

        BPlusNode plusNode = getChild(0);
        while (true) {
            if (plusNode instanceof LeafNode ) {
                return (LeafNode)plusNode;
            }else if (plusNode instanceof InnerNode ) {
                plusNode = plusNode.getLeftmostLeaf();
            }
        }

    }

    /**
     * 看起来只有分裂的键才会加入当前节点
     *
     * 分裂的时候会分为3个节点 返回root  然后把正常的节点挂在root下边
     * @param key
     * @param rid
     * @return
     */
    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // TODO(proj2): implement
        //需要找到个节点加入 好像看起来get就是我需要的方法
        BPlusNode leafNode = getOneLevel(key);

        int currentMaxNumber = tryGetMaxNumber();


        //子节点分裂
        Optional<Pair<DataBox, Long>> leafReturnValue = leafNode.put(key, rid);
        if (leafReturnValue.isPresent()) {
            Pair<DataBox, Long> pair = leafReturnValue.get();
            DataBox data = pair.getFirst();
            Long pageId = pair.getSecond();

            boolean whetherInsert = false;
            int size = this.keys.size();
            for (int i = 0; i < size; i++) {
                DataBox dataBox = this.keys.get(i);
                if (key.compareTo(dataBox) < 0) {
                    this.keys.add(i, data);
                    this.children.add(i+1, pageId);
                    whetherInsert = true;
                    break;
                }
            }
            //兜底插入
            if (!whetherInsert){
                this.keys.add(data);
                this.children.add(pageId);
            }

            //判断innerNode节点是否要分裂
            if (this.keys.size()>currentMaxNumber){
                return tryReBalance();
            }
        }
        sync();
        return Optional.empty();
    }

//    private Optional<Pair<DataBox, Long>> tryReBalance(DataBox key, LeafNode leafNode, Pair<DataBox, Long> pair, Long pageId) {
//        int d = this.metadata.getOrder();
//        int size = this.keys.size();
//        int rightNodeIndex = size  - d;
//        InnerNode rightNode = null;
//        List<DataBox> rightNodeDatas = new ArrayList<>();
//        List<Long> rightNodeIds = new ArrayList<>();
//        DataBox promoteKey = null;
//
//        for (int i = 0; i < size; i++) {
//            DataBox dataBox = this.keys.get(i);
//            Long id = this.children.get(i);
//            if (i<=d-1){
//            //前d个不动
//            }else if (i>=rightNodeIndex){
//                rightNodeDatas.add(dataBox);
//                rightNodeIds.add(id);
//                if (i==size-1){
//                    rightNodeIds.add(0,pageId);
//                    rightNode = buildNewNode(rightNodeDatas, rightNodeIds);
//                }
//                //最后d个需要放在右边的分裂键中
//
//            }else {
//                //中间的需要移动到分裂键中
//                promoteKey = this.keys.get(i);
//            }
//        }
//
//
//        return Optional.of(new Pair<>(promoteKey, rightNode.getPage().getPageNum()));
//
//    }

    /**
     * 内部节点分裂（rebalance）
     * 前提：在 put() 中已经：
     *   1. 找到 index
     *   2. keys.add(index, promoteKey)
     *   3. children.add(index + 1, newChildPage)
     *   4. 如果 keys.size() > 2*d → 调用此方法
     */
    private Optional<Pair<DataBox, Long>> tryReBalance() {
        int d = this.metadata.getOrder();
        int numKeys = this.keys.size();

        // 不满 2d+1，不需要分裂
        if (numKeys <= 2 * d) {
            return Optional.empty();
        }

        int mid = d;  // 中间键索引（第 d 个键提升）

        // 1. 取出提升键（稍后从左节点删除）
        DataBox promoteKey = this.keys.get(mid);

        // 2. 创建右节点（拿 mid+1 到末尾）
        List<DataBox> rightKeys = new ArrayList<>(this.keys.subList(mid + 1, numKeys));
        List<Long> rightChildren = new ArrayList<>(this.children.subList(mid + 1, numKeys + 1));  // 孩子比键多1

            InnerNode rightNode = buildNewNode(rightKeys, rightChildren);


            // 3. 清空当前节点（左节点）的右半部分
            this.keys.subList(mid, numKeys).clear();
            this.children.subList(mid + 1, numKeys + 1).clear();

            // 4. 移除提升键（标准 B+ 树：提升键不留在左右子节点）
//            this.keys.remove(mid);

            // 5. 保存
            this.sync();
            rightNode.sync();

            // 6. 返回提升键 + 右节点页号
            return Optional.of(new Pair<>(promoteKey, rightNode.getPage().getPageNum()));

    }

    private InnerNode buildNewNode(List<DataBox> keys, List<Long> ids) {
        // 创建新右内部节点
        Page newPage = bufferManager.fetchNewPage(this.treeContext, metadata.getPartNum());
        try {
            return new InnerNode(this.metadata, this.bufferManager, keys, ids, this.treeContext);
        } finally {
            newPage.unpin();  // 释放 pin
        }    }

    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor) {
        // TODO(proj2): implement

        // 当前叶子节点能容纳的最大值
        int currentMaxNumber = (int) Math.ceil(tryGetMaxNumber() * fillFactor);  // 或直接 tryGetMaxNumber() 如果 fillFactor 只用于叶

        while (data.hasNext()) {

            BPlusNode child = getChild(this.children.size() - 1);
            Optional<Pair<DataBox, Long>> splitterKey = child.bulkLoad(data, fillFactor);

            if (splitterKey.isPresent()){
                // 获取孩子的传播键和页号
                Pair<DataBox, Long> childPair = splitterKey.get();



                // 检查是否溢出
                if (this.keys.size() >= currentMaxNumber) {
                    // 分裂当前内部节点（假设 splitIntoTwo() 返回新右内部节点）
                    InnerNode rightInner = this.buildNewNode(child,childPair);

                    // 提升键（通常是右节点的第一个键；根据项目 spec 调整）
                    DataBox promoteKey = rightInner.keys.get(0);

                    // 保存分裂后的节点
                    rightInner.sync();
                    this.sync();

                    // 返回提升键和新右节点的页号，继续向上传播
                    return Optional.of(new Pair<>(promoteKey, rightInner.getPage().getPageNum()));
                }else {
                    // 插入到当前内部节点的末尾（数据有序）
                    this.keys.add(childPair.getFirst());
                    this.children.add(childPair.getSecond());
                    this.sync();  // 保存更改
                }

            }else {
                return Optional.empty();
            }

        }

        return Optional.empty();

    }

    private InnerNode buildNewNode(BPlusNode child, Pair<DataBox, Long> childPair) {
        // 计算分裂点（通常中点后一个，左持 floor((2d+1)/2) -1 键，右持剩余）
        int mid = (this.keys.size() + 1) / 2;  // 提升键是 mid-1，或根据 spec 调整

        // 创建新右内部节点
        Page newPage = bufferManager.fetchNewPage(this.treeContext, metadata.getPartNum());
        try {
            Pair<DataBox, Long> pair = childPair;
            DataBox dataBox = pair.getFirst();
            Long recordId = pair.getSecond();
            ArrayList<DataBox> keys = new ArrayList<>();
            keys.add(dataBox);
            ArrayList<Long> ids = new ArrayList<>();
            ids.add(child.getPage().getPageNum());
            ids.add(recordId);

            InnerNode rightInner = new InnerNode(this.metadata, this.bufferManager, keys, ids, this.treeContext);

            // 移动右半键和孩子到新节点（键从 mid 开始，孩子从 mid+1）
//            rightInner.keys.addAll(this.keys.subList(mid, this.keys.size()));
//            rightInner.children.addAll(this.children.subList(mid + 1, this.children.size()));

            // 清空原节点的右半
//            this.keys.subList(mid, this.keys.size()).clear();
//            this.children.subList(mid + 1, this.children.size()).clear();

            // 提升键是 this.keys.get(mid - 1)，但在 bulkLoad 中可能直接用右首键；这里移除提升键
//            this.keys.remove(mid - 1);  // 如果提升键从中移除（标准 B+ 树分裂）

            return rightInner;
        } finally {
            newPage.unpin();  // 释放 pin
        }
    }

//    private InnerNode splitIntoTwo() {
//        // 获取新页，并用 try-finally 管理 unpin
//        Page newPage = this.bufferManager.fetchNewPage(treeContext, metadata.getPartNum());
//        try {
//            LeafNode rightNode = LeafNode.fromBytes(this.metadata, this.bufferManager, treeContext, newPage.getPageNum());
//            rightNode.put(pair.getFirst(), pair.getSecond());  // 添加到新节点
//            rightNode.rightSibling = this.rightSibling;
//            this.rightSibling = Optional.of(newPage.getPageNum());
//            rightNode.sync();
//            this.sync();  // 保存当前节点
//            return Optional.of(new Pair<>(pair.getFirst(), newPage.getPageNum()));
//        } finally {
//            newPage.unpin();  // 关键：使用后 unpin
//        }
//    }


    /**
     *
     * @return 当前节点能容纳的最大值
     */
    private int tryGetMaxNumber() {
        return metadata.getOrder() * 2;
    }

    // See BPlusNode.remove.
    @Override
    public void remove(DataBox key) {
        // TODO(proj2): implement
        getOneLevel(key).remove(key);
        return;
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    private BPlusNode getChild(int i) {
        long pageNum = children.get(i);
        return BPlusNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<Long> getChildren() {
        return children;
    }
    /**
     * Returns the largest number d such that the serialization of an InnerNode
     * with 2d keys will fit on a single page.
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 4 + (n * keySize) + ((n + 1) * 8)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - 8 is the number of bytes used to store a child pointer.
        //
        // Solving the following equation
        //
        //   5 + (n * keySize) + ((n + 1) * 8) <= pageSizeInBytes
        //
        // we get
        //
        //   n = (pageSizeInBytes - 13) / (keySize + 8)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + 8);
        return n / 2;
    }

    /**
     * Given a list ys sorted in ascending order, numLessThanEqual(x, ys) returns
     * the number of elements in ys that are less than or equal to x. For
     * example,
     *
     *   numLessThanEqual(0, Arrays.asList(1, 2, 3, 4, 5)) == 0
     *   numLessThanEqual(1, Arrays.asList(1, 2, 3, 4, 5)) == 1
     *   numLessThanEqual(2, Arrays.asList(1, 2, 3, 4, 5)) == 2
     *   numLessThanEqual(3, Arrays.asList(1, 2, 3, 4, 5)) == 3
     *   numLessThanEqual(4, Arrays.asList(1, 2, 3, 4, 5)) == 4
     *   numLessThanEqual(5, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *   numLessThanEqual(6, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *
     * This helper function is useful when we're navigating down a B+ tree and
     * need to decide which child to visit. For example, imagine an index node
     * with the following 4 keys and 5 children pointers:
     *
     *     +---+---+---+---+
     *     | a | b | c | d |
     *     +---+---+---+---+
     *    /    |   |   |    \
     *   0     1   2   3     4
     *
     * If we're searching the tree for value c, then we need to visit child 3.
     * Not coincidentally, there are also 3 values less than or equal to c (i.e.
     * a, b, c).
     */
    static <T extends Comparable<T>> int numLessThanEqual(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) <= 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    static <T extends Comparable<T>> int numLessThan(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) < 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(children.get(i)).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(children.get(children.size() - 1)).append(")");
        return sb.toString();
    }

    @Override
    public String toSexp() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(getChild(i).toSexp()).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(getChild(children.size() - 1).toSexp()).append(")");
        return sb.toString();
    }

    /**
     * An inner node on page 0 with a single key k and two children on page 1 and
     * 2 is turned into the following DOT fragment:
     *
     *   node0[label = "<f0>|k|<f1>"];
     *   ... // children
     *   "node0":f0 -> "node1";
     *   "node0":f1 -> "node2";
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("<f%d>", i));
            ss.add(keys.get(i).toString());
        }
        ss.add(String.format("<f%d>", keys.size()));

        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        String node = String.format("  node%d[label = \"%s\"];", pageNum, s);

        List<String> lines = new ArrayList<>();
        lines.add(node);
        for (int i = 0; i < children.size(); ++i) {
            BPlusNode child = getChild(i);
            long childPageNum = child.getPage().getPageNum();
            lines.add(child.toDot());
            lines.add(String.format("  \"node%d\":f%d -> \"node%d\";",
                                    pageNum, i, childPageNum));
        }

        return String.join("\n", lines);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // When we serialize an inner node, we write:
        //
        //   a. the literal value 0 (1 byte) which indicates that this node is not
        //      a leaf node,
        //   b. the number n (4 bytes) of keys this inner node contains (which is
        //      one fewer than the number of children pointers),
        //   c. the n keys, and
        //   d. the n+1 children pointers.
        //
        // For example, the following bytes:
        //
        //   +----+-------------+----+-------------------------+-------------------------+
        //   | 00 | 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03 | 00 00 00 00 00 00 00 07 |
        //   +----+-------------+----+-------------------------+-------------------------+
        //    \__/ \___________/ \__/ \_________________________________________________/
        //     a         b        c                           d
        //
        // represent an inner node with one key (i.e. 1) and two children pointers
        // (i.e. page 3 and page 7).

        // All sizes are in bytes.
        assert (keys.size() <= 2 * metadata.getOrder());
        assert (keys.size() + 1 == children.size());
        int isLeafSize = 1;
        int numKeysSize = Integer.BYTES;
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int childrenSize = Long.BYTES * children.size();
        int size = isLeafSize + numKeysSize + keysSize + childrenSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (Long child : children) {
            buf.putLong(child);
        }
        return buf.array();
    }

    /**
     * Loads an inner node from page `pageNum`.
     */
    public static InnerNode fromBytes(BPlusTreeMetadata metadata,
                                      BufferManager bufferManager, LockContext treeContext, long pageNum) {
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buf = page.getBuffer();

        byte nodeType = buf.get();
        assert(nodeType == (byte) 0);

        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        int n = buf.getInt();
        for (int i = 0; i < n; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getLong());
        }
        return new InnerNode(metadata, bufferManager, page, keys, children, treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InnerNode)) {
            return false;
        }
        InnerNode n = (InnerNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
               keys.equals(n.keys) &&
               children.equals(n.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, children);
    }
}

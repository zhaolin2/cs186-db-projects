package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.databox.TypeId;
import edu.berkeley.cs186.database.table.Record;

/** Metadata about a B+ tree. */
public class BPlusTreeMetadata {
    // Table for which this B+ tree is for
    private final String tableName;

    // Column that this B+ tree uses as a search key
    private final String colName;

    // B+ trees map keys (of some type) to record ids. This is the type of the
    // keys.
    private final Type keySchema;

    // The order of the tree. Given a tree of order d, its inner nodes store
    // between d and 2d keys and between d+1 and 2d+1 children pointers. Leaf
    // nodes store between d and 2d (key, record id) pairs. Notable exceptions
    // include the root node and leaf nodes that have been deleted from; these
    // may contain fewer than d entries.
    //// 树的阶数。给定一个阶数为 d 的树，其内部节点存储 d 到 2d 个键，以及 d+1 到 2d+1 个子节点指针。
    // 叶节点存储 d 到 2d 个（键，记录 ID）对。
    // 值得注意的例外情况包括根节点和已被删除的叶节点，这些节点可能包含少于 d 个条目。
    private final int order;

    // The partition that the B+ tree allocates pages from. Every node of the B+ tree
    // is stored on a different page on this partition.
    //// B+ 树从中分配页面的分区。B+ 树的每个节点都存储在此分区的不同页面上。
    private final int partNum;

    // The page number of the root node.
    private long rootPageNum;

    // The height of this tree.
    private int height;

    public BPlusTreeMetadata(String tableName, String colName, Type keySchema, int order, int partNum,
                             long rootPageNum, int height) {
        this.tableName = tableName;
        this.colName = colName;
        this.keySchema = keySchema;
        this.order = order;
        this.partNum = partNum;
        this.rootPageNum = rootPageNum;
        this.height = height;
    }

    public BPlusTreeMetadata(Record record) {
        this.tableName = record.getValue(0).getString();
        this.colName = record.getValue(1).getString();
        this.order = record.getValue(2).getInt();
        this.partNum = record.getValue(3).getInt();
        this.rootPageNum = record.getValue(4).getLong();
        this.height = record.getValue(7).getInt();
        int typeIdIndex = record.getValue(5).getInt();
        int typeSize = record.getValue(6).getInt();
        this.keySchema = new Type(TypeId.values()[typeIdIndex], typeSize);
    }

    /**
     * @return a record containing this B+ tree's metadata. Useful for serializing
     * metadata about the tree (see Database#getIndexInfoSchema).
     */
    public Record toRecord() {
        return new Record(tableName, colName, order, partNum, rootPageNum,
                keySchema.getTypeId().ordinal(), keySchema.getSizeInBytes(),
                height
        );
    }

    public String getTableName() {
        return tableName;
    }

    public String getColName() {
        return colName;
    }

    public String getName() {
        return tableName + "," + colName;
    }

    public Type getKeySchema() {
        return keySchema;
    }

    public int getOrder() {
        return order;
    }

    public int getPartNum() {
        return partNum;
    }

    public long getRootPageNum() {
        return rootPageNum;
    }

    void setRootPageNum(long rootPageNum) {
        this.rootPageNum = rootPageNum;
    }

    public int getHeight() {
        return height;
    }

    void incrementHeight() {
        ++height;
    }
}

package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Simple Nested Loop Join algorithm.
 *
 * 使用简单嵌套循环连接算法，分别基于 leftColumnName 和 rightColumnName 对两个关系执行等值连接。
 */
public class SNLJOperator extends JoinOperator {
    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
              leftColumnName, rightColumnName, transaction, JoinType.SNLJ);
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SNLJIterator();
    }

    /**
     * 假设一个悲观场景
     * 对于左边的任意一条记录 都需要从磁盘中重新扫描右边的所有数据
     * @return
     */
    @Override
    public int estimateIOCost() {
        //获取左边的所有记录数
        int numLeftRecords = getLeftSource().estimateStats().getNumRecords();
        //获取右边的磁盘的页数
        int numRightPages = getRightSource().estimateStats().getNumPages();
        //每一条扫描右边的成本数 + 左边的扫描的成本数
        return numLeftRecords * numRightPages + getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Note that the left table is the "outer" loop and the right table is the
     * "inner" loop.
     */
    private class SNLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left relation
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right relation
        private BacktrackingIterator<Record> rightSourceIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        public SNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            if (leftSourceIterator.hasNext()) leftRecord = leftSourceIterator.next();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            if (leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }
            //需要一直往下循环
            while(true) {

                //右边数据源还有的话就一直遍历
                if (this.rightSourceIterator.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightSourceIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                    //左边数据源找下一个
                } else if (leftSourceIterator.hasNext()){
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    this.leftRecord = leftSourceIterator.next();
                    this.rightSourceIterator.reset();
                } else {
                    // if you're here then there are no more records to fetch
                    return null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            //为空则拿下一条记录
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }

}


package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 *
 * 页面嵌套算法
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Page Nested Loop Join algorithm.
 *
 * * 使用页面嵌套循环连接算法，分别在 leftColumnName 和 rightColumnName 上对两个关系执行等值连接。
 *
 * *
 *
 */
public class PNLJOperator extends BNLJOperator {
    public PNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction);

        joinType = JoinType.PNLJ;
        numBuffers = 3;
    }

    @Override
    public Iterator<Record> iterator() {
        return new PNLJIterator();
    }

    @Override
    public int estimateIOCost() {
//        return super.estimateIOCost();
        return getLeftSource().estimateIOCost()
                + getLeftSource().estimateStats().getNumPages() * getRightSource().estimateStats().getNumPages();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class PNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages 当前block的记录
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page 当前page的记录
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private PNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            /*
              需要一个page的去load
             */
            this.fetchNextRightPage();

            this.nextRecord = null;

        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         *
         * * 从左侧数据源获取下一个记录块。
         *
         * * `leftBlockIterator` 应设置为最多回溯到左侧数据源 B-2 页记录的迭代器，
         * 并且 `leftRecord` 应设置为
         *
         * * 此记录块中的第一条记录。
         *
         * *
         *
         * * 如果左侧数据源中没有更多记录，则此方法
         *
         * 不执行任何操作。
         *
         * *
         *  #{@link QueryOperator#getBlockIterator(Iterator, Schema, int)}
         * 您可能会发现 `QueryOperator#getBlockIterator` 在这里很有用。
         *
         * * 请确保将正确的模式传递给此方法。
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement

            /**
             * 左表需要一个一个块的进行加载
             * 假如一共100个记录
             */
            //一个块所包含的记录数
            int usableBuffers = numBuffers - 2;


//            int pageSize = getLeftSource().estimateStats().getNumPages();
//            int recordsNumberInOnePage = getLeftSource().estimateStats().getNumRecords() / pageSize;

//            Integer loadSize = recordsNumberInOnePage * usableBuffers;


            Schema schema = getLeftSource().getSchema();
//            int numPages = getLeftSource().estimateStats().getNumPages();
            this.leftBlockIterator = QueryOperator.getBlockIterator(this.leftSourceIterator, schema, usableBuffers);
            this.leftRecord = leftBlockIterator.next();

//            this.leftBlockIterator = getLeftSource().iterator();

        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         *  #{@link QueryOperator#getBlockIterator(Iterator, Schema, int)}
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         *
         * 需要一次性加载一个页所包含的记录数
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement

//            int pageSize = getRightSource().estimateStats().getNumPages();
//            int loadSize = getRightSource().estimateStats().getNumRecords() / pageSize;

            Schema schema = getRightSource().getSchema();
//            int numPages = getLeftSource().estimateStats().getNumPages();
            this.rightPageIterator = QueryOperator.getBlockIterator(this.rightSourceIterator, schema, 1);
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         *
         * * 返回此连接应生成的下一条记录，
         *
         * * 如果没有更多记录要连接，则返回 null。
         *
         * #{@link JoinOperator#compare(Record, Record)}
         * * 您可能会发现 JoinOperator#compare 在这里很有用。（您可以直接从此文件中调用 compare 函数，
         *
         * * 因为 BNLJOperator 是 JoinOperator 的子类。）
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement

            if(Objects.isNull(leftRecord)) {
                return null;
            }

            while (true){
                //右边数据源还有的话就一直遍历
                if (this.rightPageIterator.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightPageIterator.next();
                    if (Objects.isNull(rightRecord)) {
                        fetchNextRightPage();
                        rightRecord = rightPageIterator.next();
                    }
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                    //左边数据源找下一个
                } else if (leftBlockIterator.hasNext()){
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    this.leftRecord = leftBlockIterator.next();
                    if (Objects.isNull(leftRecord)) {
                        fetchNextLeftBlock();
                        this.leftRecord = leftBlockIterator.next();
                        if (Objects.isNull(leftRecord)) {
                            return null;
                        }
                    }
                    this.rightSourceIterator.reset();
                    fetchNextRightPage();
                }else if (leftSourceIterator.hasNext()){
                    fetchNextLeftBlock();
                    this.leftRecord = leftBlockIterator.next();
                    if (Objects.isNull(leftRecord)) {
                        return null;
                    }
                    this.rightSourceIterator.reset();
                    fetchNextRightPage();
                }else {
                    // if you're here then there are no more records to fetch
                    return null;
                }
            }

        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }

}

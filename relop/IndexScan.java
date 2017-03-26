package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator
{
	private HashIndex index;
	private HeapFile file;
	private BucketScan scan;
	private RID prevRID = new RID();

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public IndexScan(Schema schema, HashIndex index, HeapFile file)
	{
		this.schema = schema;
		this.index = index;
		this.file = file;
		scan = index.openScan();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth)
	{
		System.out.println("An Index Scan.  Scans a bucket scan.");
		indent(depth + 1);
		System.out.println("A Bucket Scan.  Scans the hash index.");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart()
	{
		scan.close();
		scan = index.openScan();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen()
	{
		return scan != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close()
	{
		scan.close();
		scan = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext()
	{
		return scan.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 *
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext()
	{
		if (!scan.hasNext()) throw new IllegalStateException();
		prevRID = scan.getNext();
		return new Tuple(schema, prevRID);
	}

	/**
	 * Gets the key of the last tuple returned.
	 */
	public SearchKey getLastKey()
	{
		return new SearchKey(prevRID.toString());
	}

	/**
	 * Returns the hash value for the bucket containing the next tuple, or maximum
	 * number of buckets if none.
	 */
	public int getNextHash()
	{
		return scan.getNextHash();
	}

}

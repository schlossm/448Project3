package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator
{
	private SearchKey   key;
	private HashScan    hashScan;

	private HashIndex   index;
	private HeapFile    file;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file)
	{
		this.schema = schema;
		this.key = key;
		this.index = index;
		this.file = file;
		hashScan = index.openScan(key);
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth)
	{
		System.out.println("A Key Scan.  Scans a Hash Scan.");
		indent(depth + 1);
		System.out.println("A Hash Scan.  Scans a hash map using a  Search Key.");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart()
	{
		hashScan.close();
		hashScan = index.openScan(key);
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen()
	{
		return hashScan != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close()
	{
		hashScan.close();
		hashScan = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext()
	{
		return hashScan.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 *
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext()
	{
		if (!hasNext()) throw new IllegalStateException();
		RID rid = hashScan.getNext();
		return new Tuple(schema, rid);
	}

}

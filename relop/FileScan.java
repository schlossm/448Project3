package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator
{
	private HeapScan scan = null;
	private HeapFile file;
	private RID prevRID = new RID();

	/**
	 * Constructs a file scan, given the schema and heap file.
	 */
	public FileScan(Schema schema, HeapFile file)
	{
		this.schema = schema;
		this.file = file;
		scan = file.openScan();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth)
	{
		System.out.println("A File scan object.  Scans the Heap Scan from the Heap File");
		indent(depth + 1);
		System.out.println("A Heap Scan.  Scans a Heap File.");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart()
	{
		scan.close();
		scan = file.openScan();
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
		byte[] info = scan.getNext(prevRID);
		return new Tuple(schema, info);
	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID()
	{
		return prevRID;
	}

}

package simpledb;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*; //to use ArrayList later

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int bp_maxPages;
    private ConcurrentHashMap<PageId, Page> bp_map; //hashmap using PageId as key and Page as value
    //using concurrent hashmap since thread safe implementation
   
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

   //constructor
    public BufferPool(int numPages) {
    	bp_maxPages = numPages;
    	bp_map = new ConcurrentHashMap<PageId, Page>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
	// check if page is in bufferpool already, if it is then return the page
    	if(bp_map.containsKey(pid)) {
    		return bp_map.get(pid);
    	}
    	
    	else {
    		//checks if there is space in the bufferpool

    		if(bp_map.size() >= bp_maxPages) {
    			evictPage(); //this is not supposed to be implemented in lab 1
    		}
    		
    		//finding page using pid from catalog
    		DbFile bp_file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		
    		Page new_page = bp_file.readPage(pid); 
    		
    		bp_map.put(pid, new_page); //insert pid and associated page into the hashmap
    		
    		return new_page;
    	}
    	//TransactionAbortedError not thrown here yet because locking not implemented by us
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

    	DbFile bp_df = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> bp_ep = bp_df.insertTuple(tid, t);
    	
    	for (Page bp_pg1 : bp_ep)
    	{
    		bp_pg1.markDirty(true, tid);
    		//if page is not in bp_map then it is inserted
    		if (!this.bp_map.containsKey(bp_pg1.getId())) {
                this.getPage(tid, bp_pg1.getId(), Permissions.READ_WRITE); 
    		}

    		bp_map.put(bp_pg1.getId(), bp_pg1); //updating cache 
    		//concurrent hashmap does not allow duplicate keys so put can handle all updates
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	DbFile bp_df = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	ArrayList<Page> bp_ep = bp_df.deleteTuple(tid, t);
    	
    	for (Page bp_pg1 : bp_ep)
    	{
    		bp_pg1.markDirty(true, tid);
    		bp_map.put(bp_pg1.getId(), bp_pg1); //updating cache
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (PageId pid : bp_map.keySet())
    		flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
    	bp_map.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    	Page bp_fp = bp_map.get(pid);
    	
    	if (bp_fp.isDirty() != null) //check if the page is dirty by looking at transaction id
    	{
    		DbFile bp_df = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		bp_df.writePage(bp_fp);
    		bp_fp.markDirty(false, bp_fp.isDirty());
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
    	boolean evicted = false; //tracks whether any page has been evicted
    	for (PageId pid : bp_map.keySet()) {
    		try 
    		{
        		flushPage(pid);
        		bp_map.remove(pid);
        		evicted = true;
        		break;
    		}
    		catch (IOException e)
    		{
    			throw new DbException("IO Exception: Could not flush the page while evicting.");
    		}
    	}
    	
    	if (!evicted) {
    		throw new DbException("Could not evict any page");
    	}
    }
}



package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private int id;
    private TupleDesc td;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.id = f.getAbsoluteFile().hashCode(); // as instructed in getID()
        this.td = td;
        this.numPages = 0; //we calculate numPages directly in the numPages() function
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try (RandomAccessFile hf_raf = new RandomAccessFile(file, "r")) {
        	int pos = pid.getPageNumber() * BufferPool.getPageSize();
            if (pos < 0 || pos  >= file.length()) {
                throw new IllegalArgumentException("The page doesn't exist in this file.");
            }
            hf_raf.seek(pos);
            byte[] buf = new byte[BufferPool.getPageSize()];
            hf_raf.read(buf);
            hf_raf.close();
            return new HeapPage((HeapPageId) pid, buf);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	try (RandomAccessFile hf_raf = new RandomAccessFile(file, "rw")) {
	    	PageId pid = page.getId();
	    	int pos = BufferPool.getPageSize() * pid.getPageNumber();
	    	hf_raf.seek(pos);
	    	hf_raf.write(page.getPageData(), 0, BufferPool.getPageSize());
	    	hf_raf.close();
    	}
    	catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(this.file.length() / BufferPool.getPageSize());
    }
    

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
     	if  ( t == null )
    	{
    		throw new DbException("TupleDesc does not match this file");
    	}
    	HeapPage hf_hp;
    	ArrayList<Page> hf_dp = new ArrayList<Page>();
    	for (int i = 0; i < this.numPages(); i++)
    	{
    		HeapPageId pid = new HeapPageId(this.getId(), i);
    		hf_hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if (hf_hp.getNumEmptySlots() > 0) {
        		hf_hp.insertTuple(t);
        		hf_dp.add(hf_hp);
        		//break;
        	}
    	}
    	
    	if(!hf_dp.isEmpty()) {
    		return hf_dp; 
    	}
    	
         
         //if hf_dp empty then tuple not added because no free page
    	//we need to create and append new page
         HeapPageId _hpid = new HeapPageId(this.getId(), this.numPages());
         HeapPage _hp = new HeapPage(_hpid, HeapPage.createEmptyPageData());
         _hp.insertTuple(t); 
         hf_dp.add(_hp);
         
         try (RandomAccessFile hf_raf = new RandomAccessFile(this.file, "rw")) {
         int pos = BufferPool.getPageSize() * this.numPages();
         hf_raf.seek(pos);
         byte[] _hpdata = _hp.getPageData();
         hf_raf.write(_hpdata, 0, BufferPool.getPageSize());
         hf_raf.close();
         }
         catch (IOException e) {
             e.printStackTrace();
             System.exit(1);
         }
         
         this.numPages += 1;
         return hf_dp;
         
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

    	PageId pid = t.getRecordId().getPageId();
    	
    	//check if tuple exists on file
    	if  (pid == null || !t.getTupleDesc().equals(this.td) )
    	{
    		throw new DbException("Tuple does not exist in this file.");
    	}
    	

        HeapPage hf_hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        hf_hp.deleteTuple(t);
        return new ArrayList<Page> (Arrays.asList(hf_hp)); 
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	 return new HeapFileIterator(this, tid);
    }
}



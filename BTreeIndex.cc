/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
	treeHeight = 0;
    rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC rc;
	//open the file
	rc = pf.open(indexname,mode);
	//retrieve the treeHeight and rootpid
	int buf[256];
	if (pf.endPid() == 0)
	{
		rootPid = -1;
		treeHeight = 0;
		pf.write(0,buf);
	}
	else
	{
		rc = pf.read(0, buf);
		rootPid = buf[0];
		treeHeight = buf[1];
	}
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	RC rc;
	//store the rootpid and height into disk
	int buf[256];
	buf[0] = rootPid;
	buf[1] = treeHeight;
	rc = pf.write(0, buf);
	//close the file
	rc = pf.close();
	if (rc < 0) return rc;
    return 0;
}


/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	RC rc;
	BTLeafNode leafNode;
	//the tree is empty
	if (treeHeight == 0)
	{
		if (rc = leafNode.insert(key, rid) < 0)
			return rc;
		
		rootPid = pf.endPid();
		if (rc = leafNode.write(rootPid, pf) < 0)
			return rc;
		treeHeight++;
		return 0;
	}
	//the tree only contains one root node
	else if (treeHeight == 1)
	{
		//read the content of the root
		if (rc = leafNode.read(rootPid,pf) < 0) 
			return rc;
		//no overflow
		if (leafNode.getKeyCount() < MAX_LEAF_ENTRY_NUM)
		{
			leafNode.insert(key,rid);
			//wrtie the changed node to the pagefile
			if (rc = leafNode.write(rootPid, pf) < 0)
				return rc;
		}
		//overflow
		else
		{
			//insert and split the leaf node
			BTLeafNode next_leafNode;
			int sibling_key;
			rc = leafNode.insertAndSplit(key, rid, next_leafNode, sibling_key);
			PageId sibling_pid;
			sibling_pid = pf.endPid();
			leafNode.setNextNodePtr(sibling_pid);
			//write the new node and the original root node to the pagefile
			rc = next_leafNode.write(sibling_pid, pf);
			rc = leafNode.write(rootPid, pf);
			//create the new root node
			BTNonLeafNode newroot;
			rc = newroot.initializeRoot(rootPid,sibling_key,sibling_pid);
			rootPid = pf.endPid();
			//write the new root to the pagefile
			rc = newroot.write(rootPid, pf);
			treeHeight++;
			if (rc < 0) return rc;
		}
	}
	//the tree with minimum height of 2
	else
	{
		IndexCursor cursor;
		locate(key, cursor);
		//read the content of the leaf node
		if (rc = leafNode.read(cursor.pid,pf) < 0) return rc;
		//no leaf node overflow
		if (leafNode.getKeyCount() < MAX_LEAF_ENTRY_NUM)
		{
			leafNode.insert(key,rid);
			if (rc = leafNode.write(cursor.pid, pf) < 0) return rc;
		}
		//leaf node overflow
		else
		{
			//insert and split the leaf node
			BTLeafNode next_leafNode;
			int sibling_key;
			rc = leafNode.insertAndSplit(key, rid, next_leafNode, sibling_key);
			//set pageId of new node and orginal node
			PageId sibling_pid;
			sibling_pid = pf.endPid();
			leafNode.setNextNodePtr(sibling_pid);
			//write the new node and the original root node to the pagefile
			rc = next_leafNode.write(sibling_pid, pf);
			rc = leafNode.write(cursor.pid, pf);
			if (rc < 0) return rc; 

			//check if there are nonleaf overflow
			bool root_overflow = true;
			int i = 2;
			while (i <= treeHeight)
			{
				BTNonLeafNode nonLeafNode;
				PageId parent_pid = parent_node[treeHeight-i];
				if (rc = nonLeafNode.read(parent_pid, pf) < 0) return rc;
				//no nonleaf overflow
				if (nonLeafNode.getKeyCount() < MAX_LEAF_ENTRY_NUM)
				{
					rc = nonLeafNode.insert(sibling_key, sibling_pid);
					rc = nonLeafNode.write(parent_pid, pf);
					//no thread of root overflow
					root_overflow = false;
					if (rc < 0) return rc;
					break;
				}
				//nonleaf overflow
				else
				{
					BTNonLeafNode next_nonLeafNode;
					int midKey;
					rc = nonLeafNode.insertAndSplit(sibling_key, sibling_pid, next_nonLeafNode, midKey);
					//set pid of new node
					PageId next_nonLeafNode_pid = pf.endPid();
					//wrtie the new node and the changed original node the pagefile
					rc = next_nonLeafNode.write(next_nonLeafNode_pid, pf);
					rc = nonLeafNode.write(parent_pid, pf);
					if (rc < 0) return rc;
					//reset the inserted key and pid
					sibling_key = midKey;
					sibling_pid = next_nonLeafNode_pid;
				}
				i++;
			}
			//create the new root node
			if (root_overflow == true)
			{
				BTNonLeafNode newroot;
				rc = newroot.initializeRoot(rootPid,sibling_key,sibling_pid);
				rootPid = pf.endPid();
				//write the new root to the pagefile
				rc = newroot.write(rootPid, pf);
				treeHeight++;
				if (rc < 0) return rc;
			}
		}
	}
	return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
//cursor eid starting from 0
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	RC rc;
	//read the root
	BTNonLeafNode* nonleaf = new BTNonLeafNode;
	if (rc = nonleaf->read(rootPid, pf) < 0) return rc;
	parent_node[0] = rootPid;

	//locate child ptr while nonleaf node
	PageId pid;
	int pos;
	int height = 1;
	while (height < treeHeight)
	{
		rc = nonleaf->locateChildPtr(searchKey, pid, pos);
		parent_node[height] = pid;
		if (height == treeHeight -1) break;
		rc = nonleaf->read(pid, pf);
		height++;
	}
	//reach the leaf node
	cursor.pid = pid;
	BTLeafNode* leaf = new BTLeafNode;
	rc = leaf->read(pid,pf);
	rc = leaf->locate(searchKey, cursor.eid);

	delete nonleaf;
	delete leaf;

	if (rc < 0) return rc; 
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */

 //NOTE: Assume entry number starting from 0
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	RC rc;
	BTLeafNode* leafptr = new BTLeafNode;
	//read the node
	if (rc = leafptr->read(cursor.pid, pf) < 0) return rc;
	//set key and rid
	if (rc = leafptr->readEntry(cursor.eid, key, rid) < 0) return rc;
	//set the cursor pointing to the next entry
	if (cursor.eid == leafptr->getKeyCount() - 1)
	{
		cursor.pid = leafptr->getNextNodePtr();
		cursor.eid = 0;
	}
	else
		cursor.eid += 1;
	delete leafptr;
    return 0;
}

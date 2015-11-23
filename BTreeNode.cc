#include "BTreeNode.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
using namespace std;

//helper:
PageId* BTLeafNode::siblingId()
{
	char *p = buffer;
	p += PageFile::PAGE_SIZE;
	p -= sizeof(PageId);
	return (PageId *)p;
}

BTLeafNode::BTLeafNode()
{
	num = 0;
	memset(buffer,0,sizeof(buffer));
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	RC rc;
	if(rc = pf.read(pid,buffer) < 0)	
		return rc;
	return 0; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC rc;
	if(rc = pf.write(pid,buffer) < 0)
		return rc;
	return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	/*int key_num = 0;
	int *buf = (int*) buffer;
	for (int i = 0; i < MAX_LEAF_ENTRY_NUM; i++)
	{
		int k = *(buf+3*i+2);
		if (k >= 0) key_num++;
	}
	return key_num;*/
	//this number has to be stored in the buf otherwise would be lost
	int key_num = 0;
	memcpy(&key_num,buffer+PageFile::PAGE_SIZE-8,sizeof(int));
	return key_num;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	if(getKeyCount() >= MAX_LEAF_ENTRY_NUM)
		return RC_NODE_FULL;
	int eid;
	locate(key,eid);
	leafCursor *p = (leafCursor *)buffer;
	p += eid+1;
	size_t size = sizeof(leafCursor) * (getKeyCount() - eid -1);
	char *temp = (char *)malloc(size);
	memcpy(temp,p,size);
	p->rid = rid;
	p->key = key;
	p++;
	memcpy(p,temp, size);
	free(temp);
	num = getKeyCount();
	num ++;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &num, sizeof(int));
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	RC rc;
	int eid;
	locate(key, eid);
	if(eid + 1 >= (MAX_LEAF_ENTRY_NUM + 1)/2)
	{
		leafCursor *p = (leafCursor *)buffer;
		p += (MAX_LEAF_ENTRY_NUM + 1)/2;
		size_t size = sizeof(leafCursor) * (MAX_LEAF_ENTRY_NUM/2);
		leafCursor *temp = p;
		for(int i = 0; i < MAX_LEAF_ENTRY_NUM/2; i++)
		{
			if(rc = sibling.insert(p->key,p->rid) < 0)	
				return rc;
			p++;
		}
		sibling.setNextNodePtr(getNextNodePtr());
		//can't self original next ptr since we don't know sibling's page id
		sibling.insert(key,rid);
		memset(temp,0,size);
		setNextNodePtr(-1);
		RecordId rid;
		sibling.readEntry(0,siblingKey,rid);
	}
	else{
		leafCursor *p = (leafCursor *)buffer;
		p += MAX_LEAF_ENTRY_NUM/2;
		siblingKey = p->key;
		size_t size = sizeof(leafCursor) * (MAX_LEAF_ENTRY_NUM/2 +1);
		leafCursor *temp = p;
		for(int i = 0; i < MAX_LEAF_ENTRY_NUM/2 + 1; i++)
		{
			if(rc = sibling.insert(p->key,p->rid) < 0)
				return rc;
			p++;
		}
		sibling.setNextNodePtr(getNextNodePtr());
		memset(temp,0, size);
		setNextNodePtr(-1);
		insert(key,rid);
	}
	//maintain the key number
	num = getKeyCount();
	num++;
	num /= 2;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &num, sizeof(int));
	//memcpy(sibling.buffer+PageFile::PAGE_SIZE-8, &num, sizeof(int));
	//sibling.num = num;
	return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	eid = -1;
	leafCursor *p = (leafCursor *)buffer;
	num = getKeyCount();
	while(p->key <= searchKey && eid + 1 < num)
	{
		eid++;
		if(p->key == searchKey)
			return 0;
		p++;
	}
	return RC_NO_SUCH_RECORD; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	leafCursor *p = (leafCursor *)buffer;
	if(eid < 0)
		return RC_INVALID_CURSOR;
	p += eid;
	rid = p->rid;
	key = p->key;
	return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	return *siblingId(); 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	PageId *next = siblingId();
	*next = pid; 
	return 0; 
}


BTNonLeafNode::BTNonLeafNode()
{
	m_num = 0;
	memset(buffer,0,sizeof(buffer));
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	RC rc;
	if(rc = pf.read(pid,buffer) < 0)
		return rc;
	return 0; 	 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	RC rc;
	if(rc = pf.write(pid,buffer) < 0)
		return rc;
	return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	int key_num = 0;
	memcpy(&key_num,buffer+PageFile::PAGE_SIZE-8,sizeof(int));
	return key_num;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @param pos[OUT] the position of the pid, starting with 1.
 * @return 0 if successful. Return an error code if there is an error.
 */
 //NOTE: pos starting with 1
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid, int& pos)
{
	int count = 0;
	int key;
	int i = 4;
	m_num = getKeyCount();
	while (count < m_num)
	{
		memcpy(&key, buffer+i, sizeof(int));
		if (searchKey < key)
		{
			memcpy(&pid, buffer+i-4, sizeof(int));
			pos = count+1;
			return 0;
		}
		i += 8;
		count ++;
	}
	memcpy(&pid,buffer+i-4,sizeof(int));
	pos = count+1;
	return 0;
}



/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	m_num = getKeyCount();
	if (m_num >= MAX_LEAF_ENTRY_NUM) 
		return RC_NODE_FULL;
	//locate the key
	PageId tmp;
	int pos;
	locateChildPtr(key, tmp, pos);
	if (pos <= m_num)
	{
		int countLeft = 8*(m_num-pos+1);
		char buf[countLeft];
		memcpy(buf,buffer+(pos-1)*8+4,countLeft);
		memcpy(buffer+pos*8+4,buf,countLeft);
		
		memcpy(buffer+(pos-1)*8+4, &key, sizeof(int));
		memcpy(buffer+(pos-1)*8+8, &pid, sizeof(int));
	}
	else
	{
		memcpy(buffer+pos*8, &pid, sizeof(int));
		memcpy(buffer+pos*8-4, &key, sizeof(int));
	}
	m_num++;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &m_num, sizeof(int));
	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	PageId tmp;
	int pos;
	locateChildPtr(key,tmp,pos);

	int offset;
	int leftover;
	if (pos <= (MAX_LEAF_ENTRY_NUM+1)/2)
	{
		offset = 8*(MAX_LEAF_ENTRY_NUM-1)/2+4;
		memcpy(&midKey, &buffer+offset, sizeof(int));
		leftover = 8*(MAX_LEAF_ENTRY_NUM-1)/2+4;
		memcpy(sibling.buffer, buffer+offset+4, leftover);
		memset(buffer+offset, 0, leftover+4);
		insert(key,pid);
	}
	else if (pos == (MAX_LEAF_ENTRY_NUM+3)/2)
	{
		memcpy(&midKey, &key, sizeof(int));
		offset = 8*(MAX_LEAF_ENTRY_NUM+1)/2+4;
		leftover = 8*(MAX_LEAF_ENTRY_NUM-1)/2;
		memcpy(sibling.buffer, &pid, sizeof(int));
		memcpy(sibling.buffer+4, buffer+offset, leftover);
		memset(buffer+offset,0,leftover);
	}
	else
	{
		offset = 8*(MAX_LEAF_ENTRY_NUM+1)/2+4;
		memcpy(&midKey, buffer+offset, sizeof(int));
		leftover = 8*(MAX_LEAF_ENTRY_NUM-3)/2+4;
		memcpy(sibling.buffer, buffer+offset+4, leftover);
		memset(buffer+offset, 0, leftover+4);
		sibling.insert(key,pid);
	}
	m_num -= (MAX_LEAF_ENTRY_NUM-1)/2;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &m_num, sizeof(int));
	return 0; 
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	if (pid1 < 0 || pid2 < 0)
		return RC_INVALID_PID;
	memset(buffer, 0, sizeof(buffer));
	memcpy(buffer, &pid1, sizeof(int));
	memcpy(buffer+4, &key, sizeof(int));
	memcpy(buffer+8, &pid2, sizeof(int));
	m_num++;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &m_num, sizeof(int));
}

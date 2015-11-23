/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "Bruinbase.h"
#include "SqlEngine.h"
#include <cstdio>

#include "BTreeIndex.h"
#include "PageFile.h"
#include "BTreeNode.h"



#include <iostream>
using namespace std;

int main()
{
  // run the SQL engine taking user commands from standard input (console).
  //SqlEngine::run(stdin);
	
	RecordId a1 = {5, 5};
	RecordId a2 = {6, 6};
	RecordId a3 = {7, 7};
	RecordId a4 = {8, 8};
	RecordId a5 = {1, 1};
	RecordId a6 = {2, 2};
	RecordId a7 = {3, 3};
	RecordId a8 = {4, 4};
	RecordId a9 = {9, 9};
	RecordId a10 ={0, 0};

	BTreeIndex test;
	test.open("ppfffffpp", 'w');
	test.insert(1,a1);
	test.insert(2,a2);
	test.insert(3,a3);
	test.insert(4,a4);
	test.insert(5,a1);
	test.insert(6,a2);
	test.insert(7,a3);
	test.insert(8,a4);
	test.insert(9,a9);
	test.insert(10,a10);
	IndexCursor cursor;
	test.locate(3,cursor);
	int key;
	RecordId next;
	test.readForward(cursor, key, next);
	cout<<key;
	cout<<next.pid;
	cout<<next.sid;

	test.close();

	cout<<cursor.pid<<endl;
	cout<<cursor.eid<<endl;

	/*BTLeafNode n1;
	n1.insert(1,a1);
   	n1.insert(2,a2);
   	n1.insert(3,a3);

	BTLeafNode sibling;
	int sibling_key;
   	n1.insertAndSplit(4,a4,sibling,sibling_key);
   	cout<<sibling_key;*/
   	/*BTNonLeafNode n1;
   	PageId pid1;
   	PageId pid2;
   	int key;
   	pid1 = 5;
   	pid2 = 9;
   	key = 1;
   	n1.initializeRoot(pid1,key,pid2);
   	PageId pid;
  	pid = 6;
   	key = 2;
  	n1.insert(key,pid);
  	pid = 7;
   	key = 3;
  	n1.insert(key,pid);

  	pid = 8;
  	key = 4;
  	int midkey;
   	BTNonLeafNode sibling;
   	n1.insertAndSplit(key, pid, sibling, midkey);
   	PageId ppid;
   	int pos;
   	sibling.locateChildPtr(4,ppid,pos);*/
   
   	return 0;

 }

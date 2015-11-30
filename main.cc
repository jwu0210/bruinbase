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
 #include "BTreeIndex.h"
#include "BTreeNode.h"
#include <cstdio>


int main()
{
  // run the SQL engine taking user commands from standard input (console).
  SqlEngine::run(stdin);

  return 0;
}

// int main()
// {
//   // run the SQL engine taking user commands from standard input (console).
//   // SqlEngine::run(stdin);


// 	BTreeIndex t = BTreeIndex();
// 	t.open("rrr",'w');
// 	RecordId rid;
// 	rid.pid = 0;
// 	rid.sid = 0;
// 	for (int i = 0; i < 500; i = i+2)
// 	{	
// 		rid.sid = i;
// 		rid.pid = i;
// 		t.insert(i,rid);
// 	}
// 	t.close();

//   return 0;
// }

// void dump_buffer(char *buffer)
// {
// 	printf("buffer is:\n");
// 	for (int i = 0; i < 1024; ++i)
// 	{
// 		printf("%d", buffer[i]);
// 	}
// 	printf("\n");
// }
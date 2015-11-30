/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <set>
#include <map>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

 using namespace std;

// external functions and variables for load file and sql command parsing 
 extern FILE* sqlin;
 int sqlparse(void);

 bool reach(IndexCursor i1, IndexCursor i2)
 {
  return (i1.pid == i2.pid && (i1.eid == i2.eid || i2.eid == -1));
}

bool compDiff(int diff, SelCond::Comparator comp)
{
  switch (comp) {
    case SelCond::EQ:
    if (diff != 0) return false;
    break;
    case SelCond::NE:
    if (diff == 0) return false;
    break;
    case SelCond::GT:
    if (diff <= 0) return false;
    break;
    case SelCond::LT:
    if (diff >= 0) return false;
    break;
    case SelCond::GE:
    if (diff < 0) return false;
    break;
    case SelCond::LE:
    if (diff > 0) return false;
    break;
  }
  return true;
}

RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  BTreeIndex indexTree = BTreeIndex();
  IndexCursor index;
  bool indexed = false;
  if(indexTree.open(table + ".idx", 'r') == 0)
  {
    indexed = true;
  }

  if(indexed){
    //organize conditions:
    map<string,SelCond::Comparator> valueCond;
    set<int> EQCond;
    set<int> NECond;

    SelCond upperBound;
    upperBound.comp = SelCond::LE;
    upperBound.value = "2147483647";

    SelCond lowerBound;
    lowerBound.comp = SelCond::GE;
    lowerBound.value = "-2147483647";

    IndexCursor start;
    IndexCursor end;

    bool willReturn;

    for (int i = 0; i < cond.size(); ++i)
    {
      if (cond[i].attr == 2)
      {
        valueCond[cond[i].value] = cond[i].comp;
      }
      else
      {
        switch(cond[i].comp){
          case SelCond::EQ:
          EQCond.insert(atoi(cond[i].value));
          break;
          case SelCond::NE:
          NECond.insert(atoi(cond[i].value));
          break;
          case SelCond::GT:
          case SelCond::GE:
          {
            if(atoi(cond[i].value) > atoi(lowerBound.value))
            {
              lowerBound = cond[i];
            }
            else if((atoi(cond[i].value) == atoi(lowerBound.value)) && (lowerBound.comp == SelCond::GE))
            {
              if(cond[i].comp == SelCond::GT)
                lowerBound = cond[i];
            }
            break;
          }
          case SelCond::LT:
          case SelCond::LE:
          {
            if(atoi(cond[i].value) < atoi(upperBound.value))
            {
              upperBound = cond[i];
            }
            else if((atoi(cond[i].value) == atoi(upperBound.value)) && (upperBound.comp == SelCond::LE))
            {
              if(cond[i].comp == SelCond::LT)
                upperBound = cond[i];
            }
            break;
          }
          default:
          break;
        }
      }
    }

    //not found
    if (EQCond.size() > 1 || atoi(upperBound.value) < atoi(lowerBound.value) || 
      (atoi(upperBound.value) == atoi(lowerBound.value) && (upperBound.comp != SelCond::LE || lowerBound.comp != SelCond::GE)))
    {
      goto exit_idx_select;
    }
    if (EQCond.size() == 1)
    {
      if(NECond.find(*EQCond.begin()) != NECond.end())
        goto exit_idx_select;
      if((*EQCond.begin() > atoi(upperBound.value)) ||
       (*EQCond.begin() == atoi(upperBound.value) && upperBound.comp == SelCond::LT) ||
       (*EQCond.begin() < atoi(lowerBound.value)) ||
       (*EQCond.begin() == atoi(lowerBound.value) && lowerBound.comp == SelCond::GT))
        goto exit_idx_select;
      if(rc = indexTree.locate(*EQCond.begin(),index) < 0)
        goto exit_idx_select;
      indexTree.readForward(index,key,rid);

      if(rc = rf.read(rid,key,value) < 0)
        goto exit_idx_select;

      if(valueCond.size() != 0)
      {
        for(map<string,SelCond::Comparator>::iterator it = valueCond.begin(); it != valueCond.end(); it++)
        {
          if (!compDiff(strcmp(value.c_str(), (it->first).c_str()), it->second))
          {
            goto exit_idx_select;
          }
        }
      }


      count = 1;
      switch (attr) {
        case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
        case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
        case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
        case 4:
        fprintf(stdout, "%d\n", count);
        break;
      }
      goto exit_idx_select;
    }
    else {
      count = 0;
      rc = indexTree.locate(atoi(lowerBound.value),index);
      
      if(lowerBound.comp == SelCond::GT || rc < 0)
        indexTree.readForward(index,key,rid);
      start = index;
      
      rc = indexTree.locate(atoi(upperBound.value),index);
      if(upperBound.comp == SelCond::LE || rc < 0)
        indexTree.readForward(index,key,rid);    
      end = index;

      while(!reach(start,end)) //condition when end is start of a leaf with eid = -1
      {
        willReturn = true;
        indexTree.readForward(start,key,rid);
        if(NECond.find(key) != NECond.end())
          continue;

        if (!(attr == 4 && valueCond.size() == 0))
        {
          if(rc = rf.read(rid,key,value) < 0)
            goto exit_idx_select;
        }

        if(valueCond.size() != 0)
        {
          for(map<string,SelCond::Comparator>::iterator it = valueCond.begin(); it != valueCond.end(); it++)
          {
            if (!compDiff(strcmp(value.c_str(), (it->first).c_str()), it->second))
            {
              willReturn = false;
              break;
            }
          }
        }
        if (!willReturn)
        {
          continue;
        }

        switch (attr) {
          case 1:  // SELECT key
          fprintf(stdout, "%d\n", key);
          break;
          case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
          case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
          break;
        }
        count++;
      }
      if (attr == 4) {
        fprintf(stdout, "%d\n", count);
      }

      exit_idx_select:
      rf.close();
      indexTree.close();
      return rc;
    }
  }

  else{
      // scan the table file from the beginning
    rid.pid = rid.sid = 0;
    count = 0;
    while (rid < rf.endRid()) {
    // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

    // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) {
          case 1:
          diff = key - atoi(cond[i].value);
          break;
          case 2:
          diff = strcmp(value.c_str(), cond[i].value);
          break;
        }

      // skip the tuple if any condition is not met
        if(!compDiff(diff, cond[i].comp))
          goto next_tuple;
      }

    // the condition is met for the tuple. 
    // increase matching tuple counter
      count++;

    // print the tuple 
      switch (attr) {
    case 1:  // SELECT key
    fprintf(stdout, "%d\n", key);
    break;
    case 2:  // SELECT value
    fprintf(stdout, "%s\n", value.c_str());
    break;
    case 3:  // SELECT *
    fprintf(stdout, "%d '%s'\n", key, value.c_str());
    break;
  }

    // move to the next tuple
  next_tuple:
  ++rid;
}

  // print matching tuple count if "select count(*)"
if (attr == 4) {
  fprintf(stdout, "%d\n", count);
}
rc = 0;

  // close the table file and return
exit_select:
rf.close();
return rc;
}
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  RC     rc;
  BTreeIndex indexTree;

  fstream fin;
  fin.open(loadfile.c_str(),fstream::in);

  if(rc = rf.open(table + ".tbl", 'w') < 0)
  {
    fprintf(stderr, "Error: table %s can't be created\n", table.c_str());
    return rc;
  }
  if(index)
  {
    indexTree = BTreeIndex();
    if(rc = indexTree.open(table + ".idx", 'w') < 0)
      return rc;
  }
  string line;
  while(getline(fin, line)) 
  {
    rid = rf.endRid();
    int key;
    string value;
    parseLoadLine(line,key,value);
    if(rc = rf.append(key,value,rid) < 0) {
      fprintf(stderr, "Error: fail to add record %s\n", line.c_str());
      return rc;
    }
    if(index)
    {
      if(rc = indexTree.insert(key,rid) < 0)
        return rc;
    }

  }
  if(index)
  {
    indexTree.close();
  }
  fin.close();
  rf.close();
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
  const char *s;
  char        c;
  string::size_type loc;

    // ignore beginning white spaces
  c = *(s = line.c_str());
  while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
  key = atoi(s);

    // look for comma
  s = strchr(s, ',');
  if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
  do { c = *++s; } while (c == ' ' || c == '\t');

    // if there is nothing left, set the value to empty string
  if (c == 0) { 
    value.erase();
    return 0;
  }

    // is the value field delimited by ' or "?
  if (c == '\'' || c == '"') {
    s++;
  } else {
    c = '\n';
  }

    // get the value string
  value.assign(s);
  loc = value.find(c, 0);
  if (loc != string::npos) { value.erase(loc); }

  return 0;
}

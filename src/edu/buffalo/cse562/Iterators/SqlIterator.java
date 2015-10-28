package edu.buffalo.cse562.Iterators;

import java.util.Iterator;

import net.sf.jsqlparser.expression.*;

public interface SqlIterator {
  public Expression  getNext();
 		
}

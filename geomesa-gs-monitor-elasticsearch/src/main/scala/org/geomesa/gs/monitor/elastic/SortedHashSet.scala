/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A class to provide a sorted set interface to objects that need an ordering
 * other than their natural ordering and may be duplicated by that ordering
 * value, but still must be unique by their hash value, i.e. a priority queue
 * mixed with a hash set. Elements that map to the same ordering value are
 * sorted by their natural ordering. The `equals`, `hashCode`, and `compareTo`
 * methods must be implemented accordingly to use this structure.
 *
 * @tparam E the type of elements in this set
 */
class SortedHashSet[E <: Comparable[E] with Equals](val ordering: Ordering[E]) {

  private val unique: mutable.HashMap[E, E] = mutable.HashMap.empty
  private val ordered: util.TreeMap[E, mutable.TreeSet[E]] = new util.TreeMap(ordering)

  def add(elem: E): Unit = {
    if (elem == null) return

    // remove and replace elements on every add to ensure
    // that state not included in equals gets updated
    unique.get(elem).foreach(remove)
    unique.put(elem, elem)

    val set = ordered.get(elem)
    if (set != null) set.add(elem)
    else ordered.put(elem, mutable.TreeSet(elem)(Ordering.ordered))
  }

  def add(elems: E*): Unit = elems.foreach(add)

  def remove(elem: E): Unit = {
    unique.remove(elem).foreach { prev =>
      Option(ordered.get(prev)).foreach { set =>
        set.remove(prev)
        if (set.isEmpty) ordered.remove(prev)
      }
    }
  }

  def contains(elem: E): Boolean = unique.contains(elem)

  def isEmpty: Boolean = unique.isEmpty

  def size: Int = unique.size

  def iterator: Iterator[E] = unique.valuesIterator

  def sorted: Seq[E] = ordered.values.asScala.flatten.toSeq

  def first: Option[E] = Option(ordered.firstEntry).flatMap(_.getValue.headOption)

  def take: Option[E] = {
    val took = first
    took.foreach(remove)
    took
  }

  def takeWhile(pred: E => Boolean): Seq[E] = {
    val took = Seq.newBuilder[E]
    while (first.exists(pred)) {
      took += take.get
    }
    took.result
  }
}

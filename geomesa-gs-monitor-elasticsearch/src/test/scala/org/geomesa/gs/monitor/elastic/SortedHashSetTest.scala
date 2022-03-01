/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

import org.geomesa.gs.monitor.elastic.SortedHashSetTest.Obj
import org.specs2.mutable.Specification

class SortedHashSetTest extends Specification {

  "SortedHashSet" should {
    "add elements" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 300)

      set.add(o1, o2, null, o3, o1, o2)

      val expectedElements = Seq(o1, o2, o3)
      val elements = set.iterator.toSeq

      elements must containTheSameElementsAs(expectedElements)
    }

    "remove elements" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 300)

      set.add(o1, o2, o3)
      set.remove(null)
      set.remove(o1)
      set.remove(o1)

      val expectedElements1 = Seq(o2, o3)
      val elements1 = set.iterator.toSeq

      set.contains(o1) must beFalse
      elements1 must containTheSameElementsAs(expectedElements1)

      set.remove(o3)

      val expectedElements2 = Seq(o2)
      val elements2 = set.iterator.toSeq

      set.contains(o3) must beFalse
      elements2 must containTheSameElementsAs(expectedElements2)

      set.remove(o2)

      set must beEmpty
      set.first must beNone
      set.take must beNone
      set.contains(null) must beFalse
    }

    "get the first element" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 300)

      set.add(o3, o2, o1)

      val expectedElement = o1
      val element = set.first

      element must beSome(expectedElement)

      val expectedElements = Seq(o1, o2, o3)
      val elements = set.iterator.toSeq

      elements must containTheSameElementsAs(expectedElements)
    }

    "take the first elements" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 300)
      val o4 = Obj(4, 400)

      set.add(o3, o2, o4, o1)

      val expectedElement1 = o1
      val element1 = set.take

      element1 must beSome(expectedElement1)

      val expectedElement2 = o2
      val element2 = set.take

      element2 must beSome(expectedElement2)

      val expectedElements = Seq(o3, o4)
      val elements = set.iterator.toSeq

      elements must containTheSameElementsAs(expectedElements)

      val expectedElement3 = o3
      val element3 = set.first

      element3 must beSome(expectedElement3)
    }

    "take the first elements based on a predicate" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 300)
      val o4 = Obj(4, 200)
      val o5 = Obj(5, 500)

      set.add(o1, o2, o3, o4, o5)

      val expectedElementsTaken = Seq(o1, o2, o4)
      val elementsTaken = set.takeWhile(_.time <= 250L)

      elementsTaken must containTheSameElementsAs(expectedElementsTaken)

      val expectedElementsRemaining = Seq(o3, o5)
      val elementsRemaining = set.iterator.toSeq

      elementsRemaining must containTheSameElementsAs(expectedElementsRemaining)

      val expectedElement = o3
      val element = set.first

      element must beSome(expectedElement)
    }

    "sort elements" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 300)

      set.add(o2, o3, o1)

      val expectedElements = Seq(o1, o2, o3)
      val elements = set.sorted

      elements must beSorted(set.ordering)
      elements mustEqual expectedElements
    }

    "replace elements" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100, "A")
      val o2 = Obj(2, 200, "B")
      val o3 = Obj(3, 300, "C")
      val o4 = Obj(1, 100, "D")
      val o5 = Obj(1, 200, "E")

      set.add(o1, o2, o3, o4)

      val expectedElements1 = Seq(o4, o2, o3)
      val elements1 = set.sorted

      elements1 must beSorted(set.ordering)
      elements1 mustEqual expectedElements1
      elements1.head.value mustEqual o4.value

      set.add(o5)

      val expectedElements2 = Seq(o5, o2, o3)
      val elements2 = set.sorted

      elements2 must beSorted(set.ordering)
      elements2 mustEqual expectedElements2
      elements2.head.value mustEqual o5.value
    }

    "not allow duplicate elements by id" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(2, 300)
      val o4 = Obj(4, 400)
      val o5 = Obj(2, 150)

      set.add(o1, o2, o3, o4, o5)

      val expectedElements = Seq(o1, o5, o4)
      val elements = set.sorted

      elements must beSorted(set.ordering)
      elements mustEqual expectedElements
    }

    "allow duplicate elements by Ordering value" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 100)
      val o4 = Obj(4, 100)

      set.add(o1, o2, o3, o4)

      val expectedElements = Seq(o1, o3, o4, o2)
      val elements = set.sorted

      elements must beSorted(set.ordering)
      elements mustEqual expectedElements
    }

    "stay in ooer" in {
      val set = new SortedHashSet(Obj.timeOrdering)

      val o1 = Obj(1, 100)
      val o2 = Obj(2, 200)
      val o3 = Obj(3, 300)
      val o4 = Obj(2, 100)
      val o5 = Obj(4, 200)
      val o6 = Obj(3, 600)
      val o7 = Obj(3, 50)
      val o8 = Obj(1, 200)
      val o9 = Obj(1, 50)

      set.add(o1, o2, o2, o3, o4, o5, o6, o7, o8, o9, o5, o2, o3)

      val expectedElements = Seq(o9, o2, o5, o3)
      val elements = set.sorted

      elements must beSorted(set.ordering)
      elements mustEqual expectedElements

      set.takeWhile(_ => true)

      set must beEmpty
      set.first must beNone
      set.take must beNone
    }
  }
}

private object SortedHashSetTest {

  case class Obj(id: Long, time: Long, value: String = null) extends Comparable[Obj] with Equals {
    override def toString: String = s"Obj($id, $time, $value)"
    override def hashCode: Int = id.hashCode
    override def compareTo(that: Obj): Int = this.id.compareTo(that.id)
    override def canEqual(that: Any): Boolean = that.isInstanceOf[Obj]
    override def equals(obj: Any): Boolean = {
      obj match {
        case that: Obj if that.canEqual(this) && (this.compareTo(that) == 0) => true
        case _ => false
      }
    }
  }

  object Obj {
    val timeOrdering: Ordering[Obj] = new Ordering[Obj] {
      override def compare(o1: Obj, o2: Obj): Int = o1.time.compare(o2.time)
    }
  }
}

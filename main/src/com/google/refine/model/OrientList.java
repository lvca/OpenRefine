package com.google.refine.model;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

public class OrientList<T> implements List<T> {

  protected final ODocumentSerializer<T> serializer;
  protected final ODatabaseDocumentTx    database;
  protected final String                 className;
  protected int                          classId;

  public OrientList(final ODocumentSerializer<T> iSerializer, final String iDatabaseURL, final String iClassName,
      final String iUser, final String iPassword) {
    OGlobalConfiguration.USE_WAL.setValue(false);
    
    serializer = iSerializer;
    database = new ODatabaseDocumentTx(iDatabaseURL);
    className = iClassName;

    if (database.exists())
      database.open(iUser, iPassword);
    else
      database.create();

    init();
  }

  public OrientList(final ODocumentSerializer<T> iSerializer, final ODatabaseDocumentTx iDatabase, final String iClassName) {
    serializer = iSerializer;
    database = iDatabase;
    className = iClassName;

    init();
  }

  public void init() {
    OClass cls = database.getMetadata().getSchema().getClass(className);
    if (cls == null)
      cls = database.getMetadata().getSchema().createClass(className);

    classId = cls.getDefaultClusterId();
  }

  @Override
  public int size() {
    return (int) database.countClass(className);
  }

  @Override
  public boolean isEmpty() {
    return database.countClass(className) == 0;
  }

  @Override
  public boolean contains(final Object o) {
    return false;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      protected ORecordIteratorClass<ODocument> sub = new ORecordIteratorClass<ODocument>(database,
                                                        (ODatabaseRecordAbstract) database.getUnderlying(), className, true);

      @Override
      public boolean hasNext() {
        return sub.hasNext();
      }

      @Override
      public T next() {
        return serializer.deserialize(sub.next());
      }

      @Override
      public void remove() {
        sub.remove();
      }
    };
  }

  @Override
  public Object[] toArray() {
    return null;
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override
  public boolean add(T e) {
    serializer.serialize(e).save();
    return false;
  }

  @Override
  public boolean remove(Object o) {
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    for (T e : c)
      add(e);
    return true;
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    throw new UnsupportedOperationException("addAll(index, coll)");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    for (Object e : c)
      remove(e);
    return true;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    database.command(new OCommandSQL("truncate class " + className));
  }

  @Override
  public T get(int index) {
    return serializer.deserialize((ODocument) new ORecordId(classId, new OClusterPositionLong(index)).getRecord());
  }

  @Override
  public T set(int index, T element) {
    final T old = serializer.deserialize((ODocument) new ORecordId(classId, new OClusterPositionLong(index)).getRecord());

    final ODocument doc = serializer.serialize(element);
    ((ORecordId) doc.getIdentity()).clusterPosition = new OClusterPositionLong(index);
    doc.save();

    return old;
  }

  @Override
  public void add(int index, T element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o) {
    return 0;
  }

  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  @Override
  public ListIterator<T> listIterator() {
    return null;
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    return null;
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return null;
  }

  public String getClassName() {
    return className;
  }

  public ODatabaseDocumentTx getDatabase() {
    return database;
  }
}

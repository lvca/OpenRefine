package com.google.refine.model;

import com.orientechnologies.orient.core.record.impl.ODocument;

public interface ODocumentSerializer<T> {
  public ODocument serialize(T iObject);

  public T deserialize(ODocument iDocument);
}

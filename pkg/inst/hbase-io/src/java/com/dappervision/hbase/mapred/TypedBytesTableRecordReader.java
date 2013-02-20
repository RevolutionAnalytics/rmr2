/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dappervision.hbase.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.typedbytes.TypedBytesOutput;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.hbase.mapred.TableRecordReaderImpl;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.record.Buffer;
import java.util.TreeMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Iterate over an HBase table data, return (Text, RowResult) pairs
 */
public class TypedBytesTableRecordReader
    implements RecordReader<TypedBytesWritable, TypedBytesWritable> {

  protected TableRecordReaderImpl recordReaderImpl = new TableRecordReaderImpl();

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   * @throws IOException
   */
  public void restart(byte[] firstRow) throws IOException {
    this.recordReaderImpl.restart(firstRow);
  }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   */
  public void init() throws IOException {
      this.recordReaderImpl.init();
  }

  /**
   * @param htable the {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
    this.recordReaderImpl.setHTable(htable);
  }

  /**
   * @param inputColumns the columns to be placed in {@link TypedBytesWritable}.
   */
  public void setInputColumns(final byte [][] inputColumns) {
    this.recordReaderImpl.setInputColumns(inputColumns);
  }

  /**
   * @param startRow the first row in the split
   */
  public void setStartRow(final byte [] startRow) {
    this.recordReaderImpl.setStartRow(startRow);
  }

  /**
   *
   * @param endRow the last row in the split
   */
  public void setEndRow(final byte [] endRow) {
    this.recordReaderImpl.setEndRow(endRow);
  }

  /**
   * @param rowFilter the {@link Filter} to be used.
   */
  public void setRowFilter(Filter rowFilter) {
    this.recordReaderImpl.setRowFilter(rowFilter);
  }

  public void close() {
    this.recordReaderImpl.close();
  }

  /**
   * @return TypedBytesWritable
   *
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public TypedBytesWritable createKey() {
      //return this.recordReaderImpl.createKey();
      return new TypedBytesWritable();
  }

  /**
   * @return RowTypedBytesWritable
   *
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public TypedBytesWritable createValue() {
      //return this.recordReaderImpl.createValue();
      return new TypedBytesWritable();
  }

  public long getPos() {

    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return this.recordReaderImpl.getPos();
  }

  public float getProgress() {
    // Depends on the total number of tuples and getPos
    return this.recordReaderImpl.getPos();
  }

  /**
   * @param key HStoreKey as input key.
   * @param value MapWritable as input value
   * @return true if there was more data
   * @throws IOException
   */
  public boolean next(TypedBytesWritable key, TypedBytesWritable value)
  throws IOException {
      ImmutableBytesWritable key0 = new ImmutableBytesWritable();
      Result value0 = new Result();
      boolean out = this.recordReaderImpl.next(key0, value0);
      if (out) {
	  TreeMap tm = new TreeMap();
	  for (Map.Entry<byte [], NavigableMap<byte [], byte []>> entry : value0.getNoVersionMap().entrySet()) {
	      TreeMap tm_inner = new TreeMap();
	      for (Map.Entry<byte [], byte []> entry0 : entry.getValue().entrySet()) {
		  tm_inner.put(new Buffer(entry0.getKey()), new Buffer(entry0.getValue()));
	      }
	      tm.put(new Buffer(entry.getKey()), tm_inner);
	  }
	  key.setValue(new Buffer(key0.get()));
	  value.setValue(tm);
      }
      return out;

  }
}
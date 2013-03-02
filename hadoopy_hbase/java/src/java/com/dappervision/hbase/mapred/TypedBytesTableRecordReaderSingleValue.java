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

public class TypedBytesTableRecordReaderSingleValue extends TypedBytesTableRecordReader {
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
          byte [] value_byte = value0.value();
          if (value_byte == null) {
              throw new IOException("SingleValue requires at least one column to be present for each row, this should not be possible!");
          }
	  key.setValue(new Buffer(key0.get()));
	  value.setValue(new Buffer(value_byte));
      }
      return out;

  }
}

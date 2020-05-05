/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.math2.utils;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;


public abstract class DataBlock<VALUE> {
    private static final Log LOG = LogFactory.getLog(DataBlock.class);

    protected Class<VALUE> valueClass;
    protected volatile int readIndex;
    protected volatile int writeIndex;

    //对索引的操作同时使用了volatile和synchronized
    public synchronized void incrReadIndex() {
        readIndex++;
    }

    public synchronized void decrReadIndex() {
        readIndex--;
    }

    public synchronized void incrWriteIndex() {
        writeIndex++;
    }

    public synchronized void decrWriteIndex() {
        writeIndex--;
    }

    public DataBlock() {
        readIndex = 0;
        writeIndex = 0;
    }

    /**
     * 设置存储对象类型
     *
     * @param valueClass 泛型
     */
    public void registerType(Class<VALUE> valueClass) {
        this.valueClass = valueClass;
    }

    /**
     * 将读索引加1，然后读取读索引位置的对象，如果已经读到末尾，直接返回null
     *
     * @return 下一个对象，若返回值为null，表明已经读到末尾
     * @throws IOException
     */
    public abstract VALUE read() throws IOException;

    protected abstract boolean hasNext() throws IOException;

    /**
     * 读取指定位置的对象，目前仅有MemoryDataBlock实现了该接口，该接口不会修改读索引
     *
     * @param index 对象索引
     * @return 下一个对象，若返回值为null，表明已经读到末尾
     * @throws IOException
     */
    public abstract VALUE get(int index) throws IOException;

    /**
     * 在写索引位置添加一个新元素，然后将写索引加1
     *
     * @param value 待添加对象
     * @throws IOException
     */
    public abstract void put(VALUE value) throws IOException;

    /**
     * 将读索引置为0，下次调用read会从头开始读取
     *
     * @throws IOException
     */
    public abstract void resetReadIndex() throws IOException;

    /**
     * 删除所有对象，并将读写索引置为0
     *
     * @throws IOException
     */
    public abstract void clean() throws IOException;

    /**
     * 随机打乱对象的顺序，目前仅MemoryDataBlock支持该操作
     *
     * @throws IOException
     */
    public abstract void shuffle() throws IOException;

    public abstract void flush() throws IOException;

    public abstract DataBlock<VALUE> slice(int startIndex, int length) throws IOException;

    public Class<VALUE> getValueClass() {
        return valueClass;
    }

    public void setValueClass(Class<VALUE> valueClass) {
        this.valueClass = valueClass;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public void setReadIndex(int readIndex) {
        this.readIndex = readIndex;
    }

    public int getWriteIndex() {
        return writeIndex;
    }

    public void setWriteIndex(int writeIndex) {
        this.writeIndex = writeIndex;
    }

    public int size() {
        return writeIndex;
    }

    public float getProgress() {
        if (writeIndex == 0) {
            return 0.0f;
        }
        return (float) readIndex / writeIndex;
    }

    /**
     * 调用Read，如果读到末尾，调用resetIndex方法，从头开始再读，保证一定能够读到一个值
     *
     * @return 下一个对象
     * @throws Exception
     */
    public VALUE loopingRead() throws Exception {
        VALUE data = this.read();
        if (data == null) {
            resetReadIndex();
            data = read();
        }

        if (data != null)
            return data;
        else
            throw new Exception("Train data storage is empty or corrupted.");
    }

    @Override
    public String toString() {
        return "DataBlock [valueClass=" + valueClass + ", readIndex=" + readIndex + ", writeIndex="
                + writeIndex + "]";
    }
}
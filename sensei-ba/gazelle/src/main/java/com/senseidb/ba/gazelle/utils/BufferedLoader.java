package com.senseidb.ba.gazelle.utils;

import com.browseengine.bobo.util.BigIntArray;
import com.browseengine.bobo.util.BigIntBuffer;
import com.browseengine.bobo.util.BigNestedIntArray;
import com.browseengine.bobo.util.BigNestedIntArray.Loader;


  public  class BufferedLoader extends Loader
  {
    private static int EOD = Integer.MIN_VALUE;
    private static int SEGSIZE = 8;
    
    private int _size;
    private final BigIntArray _info;
    private BigIntBuffer _buffer;
    private int _maxItems;
    
    public BufferedLoader(int size, int maxItems, BigIntBuffer buffer)
    {
      _size = size;
      _maxItems = Math.min(maxItems, BigNestedIntArray.MAX_ITEMS);
      _info = new BigIntArray(size << 1); // pointer and count
      _info.fill(EOD);
      _buffer = buffer;
    }
    
    public BufferedLoader(int size)
    {
      this(size, BigNestedIntArray.MAX_ITEMS, new BigIntBuffer());
    }
    
    /**
     * resets loader. This also resets underlying BigIntBuffer.
     */
    public void reset(int size, int maxItems, BigIntBuffer buffer)
    {
      if(size >= capacity()) throw new IllegalArgumentException("unable to change size");
      _size = size;
      _maxItems = maxItems;
      _info.fill(EOD);
      _buffer = buffer;
    }
    
    /**
     * adds a pair of id and value to the buffer
     * @param id
     * @param val
     */
    public final boolean add(int id, int val)
    {
      int ptr = _info.get(id << 1);
      if(ptr == EOD)
      {
        // 1st insert
        _info.add(id << 1, val);
        return true;
      }
      
      int cnt = _info.get((id << 1) + 1);
      if(cnt == EOD)
      {
        // 2nd insert
        _info.add((id << 1) + 1, val);
        return true;
      }
      
      if(ptr >= 0)
      {
        // this id has two values stored in-line.
        int firstVal = ptr;
        int secondVal = cnt;
        
        ptr = _buffer.alloc(SEGSIZE);
        _buffer.set(ptr++, EOD);
        _buffer.set(ptr++, firstVal);
        _buffer.set(ptr++, secondVal);
        _buffer.set(ptr++, val);
        cnt = 3;
      }
      else
      {
        ptr = (- ptr);
        if (cnt >= _maxItems) return false; // exceeded the limit
      
        if((ptr % SEGSIZE) == 0)
        {
          int oldPtr = ptr;
          ptr = _buffer.alloc(SEGSIZE);
          _buffer.set(ptr++, (- oldPtr));
        }
        _buffer.set(ptr++, val);
        cnt++;
      }
      
      _info.add(id << 1, (- ptr));
      _info.add((id << 1) + 1, cnt);
      
      return true;
    }

    private final int readToBuf(int id, int[] buf)
    {
      int ptr = _info.get(id << 1);
      int cnt = _info.get((id << 1) + 1);
      int i;
      
      if(ptr >=0)
      {
        // read in-line data
        i = 0;
        buf[i++] = ptr;
        if(cnt >= 0) buf[i++] = cnt;
        return i;
      }
      
      // read from segments
      i = cnt;
      while(ptr != EOD)
      {
        ptr = (- ptr) - 1;
        int val;
        while((val = _buffer.get(ptr--)) >= 0)
        {
          buf[--i] = val;
        }
        ptr = val;
      }
      if(i > 0)
      {
        throw new RuntimeException("error reading buffered data back");
      }
      
      return cnt;
    }

    public final void load()
    {
      int[] buf = new int[BigNestedIntArray.MAX_ITEMS];
      int size = _size;
      for(int i = 0; i < size; i++)
      {
        int count = readToBuf(i, buf);
        if(count > 0)
        {
          add(i, buf, 0, count);
        }
      }
    }
    
    public final int capacity()
    {
      return _info.capacity() >> 1;
    }
  }


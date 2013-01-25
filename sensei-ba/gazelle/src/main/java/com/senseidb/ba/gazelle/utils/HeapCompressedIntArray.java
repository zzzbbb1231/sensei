package com.senseidb.ba.gazelle.utils;


/**
 * Shamelessly borrowed from Lucene
 *
 */
public class HeapCompressedIntArray implements IntArray {
  static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
  static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
  static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

  private static final int ENTRY_SIZE = BLOCK_SIZE + 1;
  private static final int FAC_BITPOS = 3;

  /*
   * In order to make an efficient value-getter, conditionals should be
   * avoided. A value can be positioned inside of a block, requiring shifting
   * left or right or it can span two blocks, requiring a left-shift on the
   * first block and a right-shift on the right block.
   * </p><p>
   * By always shifting the first block both left and right, we get exactly
   * the right bits. By always shifting the second block right and applying
   * a mask, we get the right bits there. After that, we | the two bitsets.
  */
  private static final int[][] SHIFTS =
          new int[ENTRY_SIZE][ENTRY_SIZE * FAC_BITPOS];
          //new int[BLOCK_SIZE+1][BLOCK_SIZE][BLOCK_SIZE+1];
  private static final long[][] MASKS = new long[ENTRY_SIZE][ENTRY_SIZE];

  static { // Generate shifts
      for (int elementBits = 1 ; elementBits <= BLOCK_SIZE ; elementBits++) {
          for (int bitPos = 0 ; bitPos < BLOCK_SIZE ; bitPos++) {
              int[] currentShifts = SHIFTS[elementBits];
              int base = bitPos * FAC_BITPOS;
              currentShifts[base    ] = bitPos;
              currentShifts[base + 1] = BLOCK_SIZE - elementBits;
              if (bitPos <= BLOCK_SIZE - elementBits) { // Single block
                  currentShifts[base + 2] = 0;
                  MASKS[elementBits][bitPos] = 0;
              } else { // Two blocks
                  int rBits = elementBits - (BLOCK_SIZE - bitPos);
                  currentShifts[base + 2] = BLOCK_SIZE - rBits;
                  MASKS[elementBits][bitPos] = ~(~0L << rBits);
              }
          }
      }
  }

  /*
   * The setter requires more masking than the getter.
  */
  private static final long[][] WRITE_MASKS =
          new long[ENTRY_SIZE][ENTRY_SIZE * FAC_BITPOS];
  static {
      for (int elementBits = 1 ; elementBits <= BLOCK_SIZE ; elementBits++) {
          long elementPosMask = ~(~0L << elementBits);
          int[] currentShifts = SHIFTS[elementBits];
          long[] currentMasks = WRITE_MASKS[elementBits];
          for (int bitPos = 0 ; bitPos < BLOCK_SIZE ; bitPos++) {
              int base = bitPos * FAC_BITPOS;
              currentMasks[base  ] =~((elementPosMask
                                 << currentShifts[base + 1])
                                >>> currentShifts[base]);
              if (bitPos <= BLOCK_SIZE - elementBits) { // Second block not used
                currentMasks[base+1] = ~0; // Keep all bits
                currentMasks[base+2] = 0;  // Or with 0
              } else {
                currentMasks[base+1] = ~(elementPosMask
                                         << currentShifts[base + 2]);
                currentMasks[base+2] = currentShifts[base + 2] == 0 ? 0 : ~0;
              }
          }
      }
  }

  /* The bits */
  private long[] blocks;

  // Cached calculations
  private int maxPos;      // blocks.length * BLOCK_SIZE / elementBits - 1
  private int[] shifts;    // The shifts for the current elementBits
  private long[] readMasks;
  private long[] writeMasks;
  private final int bitsPerValue;
  private final int valueCount;

  /**
   * Creates an array with the internal structures adjusted for the given
   * limits and initialized to 0.
   * @param valueCount   the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   */
  public HeapCompressedIntArray(int valueCount, int bitsPerValue) {
    // TODO: Test for edge-cases (2^31 values, 63 bitsPerValue)
    // +2 due to the avoid-conditionals-trick. The last entry is always 0
    this(new long[(int)((long)valueCount * bitsPerValue / BLOCK_SIZE + 2)],
            valueCount, bitsPerValue);
    
  }


  /**
   * Creates an array backed by the given blocks.
   * </p><p>
   * Note: The blocks are used directly, so changes to the given block will
   * affect the Packed32-structure.
   * @param blocks   used as the internal backing array. Not that the last
   *                 element cannot be addressed directly.
   * @param valueCount the number of values.
   * @param bitsPerValue the number of bits available for any given value.
   */
  public HeapCompressedIntArray(long[] blocks, int valueCount, int bitsPerValue) {
  
    this.blocks = blocks;
    this.valueCount = valueCount;
    this.bitsPerValue = bitsPerValue;
    updateCached();
  }

  

  private static int size(int valueCount, int bitsPerValue) {
    final long totBitCount = (long) valueCount * bitsPerValue;
    return (int)(totBitCount/64 + ((totBitCount % 64 == 0 ) ? 0:1));
  }

  private void updateCached() {
    readMasks = MASKS[bitsPerValue];
    shifts = SHIFTS[bitsPerValue];
    writeMasks = WRITE_MASKS[bitsPerValue];
    maxPos = (int)((((long)blocks.length) * BLOCK_SIZE / bitsPerValue) - 2);
  }

  /**
   * @param index the position of the value.
   * @return the value at the given index.
   */
  public int getInt(final int index) {
    assert index >= 0 && index < size();
    final long majorBitPos = (long)index * bitsPerValue;
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    final int bitPos =     (int)(majorBitPos & MOD_MASK); // % BLOCK_SIZE);

    final int base = bitPos * FAC_BITPOS;
    assert elementPos < blocks.length : "elementPos: " + elementPos + "; blocks.len: " + blocks.length;
    return (int)(((blocks[elementPos] << shifts[base]) >>> shifts[base+1]) |
            ((blocks[elementPos+1] >>> shifts[base+2]) & readMasks[bitPos]));
  }

  public int size() {
    return valueCount;
  }


  public void setInt(final int index, final int val) {
    long value = val;
    
    final long majorBitPos = (long)index * bitsPerValue;
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    final int bitPos =     (int)(majorBitPos & MOD_MASK); // % BLOCK_SIZE);
    final int base = bitPos * FAC_BITPOS;

    blocks[elementPos  ] = (blocks[elementPos  ] & writeMasks[base])
                           | (value << shifts[base + 1] >>> shifts[base]);
    blocks[elementPos+1] = (blocks[elementPos+1] & writeMasks[base+1])
                           | ((value << shifts[base + 2]) & writeMasks[base+2]);
  }

  @Override
  public String toString() {
    return "Packed64(bitsPerValue=" + bitsPerValue + ", size="
            + size() + ", maxPos=" + maxPos
            + ", elements.length=" + blocks.length + ")";
  }


  public long[] getBlocks() {
    return blocks;
  }


  public int getBitsPerValue() {
    return bitsPerValue;
  }
  
 
}




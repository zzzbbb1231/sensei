package com.senseidb.ba.gazelle.utils;


public class HeapCompressedThreeShortsArray implements IntArray {
	static final int BLOCK_SIZE = 16; // 32 = int, 64 = long
	static final int BLOCK_BITS = 4; // The #bits representing BLOCK_SIZE
	static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

	/**
	 * Values are stores contiguously in the blocks array.
	 */
	private final short[] blocks;
	/**
	 * A right-aligned mask of width BitsPerValue used by {@link #get(int)}.
	 */
	private final int maskRight;
	
	/**
	 * Optimization: Saves one lookup in {@link #get(int)}.
	 */
	private final int bpvMinusBlockSize;
	private final int bitsPerValue;
	private final int valueCount;

	/**
	 * Creates an array with the internal structures adjusted for the given limits
	 * and initialized to 0.
	 * 
	 * @param valueCount
	 *          the number of elements.
	 * @param bitsPerValue
	 *          the number of bits available for any given value.
	 */
	public HeapCompressedThreeShortsArray(int valueCount, int bitsPerValue) {
		// NOTE: block-size was previously calculated as
		// valueCount * bitsPerValue / BLOCK_SIZE + 1
		// due to memory layout requirements dictated by non-branching code
		this(new short[size(valueCount, bitsPerValue)], valueCount, bitsPerValue);
	}

	/**
	 * Creates an array backed by the given blocks. </p>
	 * <p>
	 * Note: The blocks are used directly, so changes to the given block will
	 * affect the Packed64-structure.
	 * 
	 * @param blocks
	 *          used as the internal backing array. Not that the last element
	 *          cannot be addressed directly.
	 * @param valueCount
	 *          the number of values.
	 * @param bitsPerValue
	 *          the number of bits available for any given value.
	 */
	public HeapCompressedThreeShortsArray(short[] blocks, int valueCount, int bitsPerValue) {
		this.blocks = blocks;
		this.valueCount = valueCount;
		this.bitsPerValue = bitsPerValue;
		maskRight = (short) (~0 << (BLOCK_SIZE - bitsPerValue + 16) >>> (BLOCK_SIZE - bitsPerValue + 16));
		bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
	}

	public int size() {
		return valueCount;
	}

	private static int size(int valueCount, int bitsPerValue) {
		final long totBitCount = (long) valueCount * bitsPerValue;
		return (int) (totBitCount / BLOCK_SIZE + ((totBitCount % 16 == 0) ? 0 : 1));
	}

	/**
	 * @param index
	 *          the position of the value.
	 * @return the value at the given index.
	 */
	public int getInt(final int index) {
		
		// The abstract index in a bit stream
		final long majorBitPos = (long) index * bitsPerValue;
		// The index in the backing long-array
		final int elementPos = (int) (majorBitPos >>> BLOCK_BITS);
		// The number of value-bits in the second long
		final long endBits = bitsPerValue - (16 - (majorBitPos & MOD_MASK));

		if (endBits <= 0) { // Single block
			return (int)((blocks[elementPos] >>> -endBits) & maskRight);
		}
		else if(endBits <= 16){
			// Two blocks
//			int highBits = blocks[elementPos] << endBits & ~(~0 << bitsPerValue - 1 << 1);
//			int lowBits = (blocks[elementPos + 1] << 16 >>> (32 - endBits));
//			return (highBits | lowBits) & maskRight;
			return (blocks[elementPos] << endBits & ~(~0 << bitsPerValue - 1 << 1) | (blocks[elementPos + 1] << 16 >>> (32 - endBits))) & maskRight;
		}
		else{
//			int highBits = blocks[elementPos] << endBits & ~(~0 << bitsPerValue - 1 << 1);
//			int midBits  = (blocks[elementPos+1] << (endBits - 16)) & (~0 >>> 16 << (endBits - 16));
//			int lowBits = blocks[elementPos+2] >>> (16 - (endBits - 16)) & ~(~0 << (endBits - 16));
//			return (highBits|midBits|lowBits) & maskRight;
			// Three blocks
			return (int)(((blocks[elementPos] << endBits & ~(~0 << bitsPerValue - 1 << 1))
						| (blocks[elementPos+1] << (endBits - 16)) & (~0 >>> 16 << (endBits - 16))
						| blocks[elementPos+2] >>> (16 - (endBits - 16)) & ~(~0 << (endBits - 16)))
					& maskRight);
		}		
	}



	public void setInt(final int index, final int value) {

		// The abstract index in a contiguous bit stream
		final long majorBitPos = (long) index * bitsPerValue;
		// The index in the backing long-array
		final int elementPos = (int) (majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
		// The number of value-bits in the second long
		final long endBits = bitsPerValue - (16 - (majorBitPos & MOD_MASK));

		if (endBits <= 0 && bitsPerValue <= 16) { // Single block
			blocks[elementPos] = (short) (blocks[elementPos] & ~(maskRight << -endBits)
					| (value << -endBits));
			return;
		}
		else if(endBits <= 16){
			// Two blocks
			blocks[elementPos] = (short) (((int) blocks[elementPos] & ~(~0 >>> 16 >>> (16 - (bitsPerValue - endBits))))
					| (value >>> endBits));
			blocks[elementPos + 1] = (short) ((blocks[elementPos + 1] & (~0 >>> (32 - endBits))) & ~(~0 >>> endBits)
					| (value << (16 - endBits)));
		}
		else{
			//Three blocks
			blocks[elementPos] = (short) (blocks[elementPos] & ~(~0 >>> 16 >>> (16 - (bitsPerValue - endBits)))
					| (value >>> 16 >>> (endBits - 16)));
			blocks[elementPos + 1] = (short) (value >>> (endBits - 16));
			blocks[elementPos + 2] = (short) (blocks[elementPos + 2] & (~maskRight >>> (15 + endBits) >>> 1)
					| (value << (16 - (endBits - 16))));
		}
	}
	public short[] getBlocks() {
		return blocks;
	}

	public int getBitsPerValue() {
		return bitsPerValue;
	}

}


package com.senseidb.ba.gazelle.utils;


public class HeapCompressedTwoIntsArray implements IntArray {
	static final int BLOCK_SIZE = 32; // 32 = int, 64 = long
	static final int BLOCK_BITS = 5; // The #bits representing BLOCK_SIZE
	static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

	/**
	 * Values are stores contiguously in the blocks array.
	 */
	private final int[] blocks;
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
	public HeapCompressedTwoIntsArray(int valueCount, int bitsPerValue) {
		// NOTE: block-size was previously calculated as
		// valueCount * bitsPerValue / BLOCK_SIZE + 1
		// due to memory layout requirements dictated by non-branching code
		this(new int[size(valueCount, bitsPerValue)], valueCount, bitsPerValue);
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
	public HeapCompressedTwoIntsArray(int[] blocks, int valueCount, int bitsPerValue) {
		this.blocks = blocks;
		this.valueCount = valueCount;
		this.bitsPerValue = bitsPerValue;
		maskRight = ~0 << (BLOCK_SIZE - bitsPerValue) >>> (BLOCK_SIZE - bitsPerValue);
		bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
	}

	public int size() {
		return valueCount;
	}

	private static int size(int valueCount, int bitsPerValue) {
		final long totBitCount = (long) valueCount * bitsPerValue;
		return (int) (totBitCount / BLOCK_SIZE + ((totBitCount % 64 == 0) ? 0 : 1));
	}

	/**
	 * @param index
	 *          the position of the value.
	 * @return the value at the given index.
	 */
	public int getInt(final int index) {
		// The abstract index in a bit stream
		final long majorBitPos = (long) index * bitsPerValue;
		// The index in the backing int-array
		final int elementPos = (int) (majorBitPos >>> BLOCK_BITS);
		// The number of value-bits left after the first int
		final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

		if (endBits <= 0) { // Single block
			return (int)((blocks[elementPos] >>> -endBits) & maskRight);
		}
		// Two blocks
		return (int)(((blocks[elementPos] << endBits) | (blocks[elementPos + 1] >>> (BLOCK_SIZE - endBits)))
				& maskRight);
	}



	public void setInt(final int index, final int value) {
		// The abstract index in a contiguous bit stream
		final long majorBitPos = (long) index * bitsPerValue;
		// The index in the backing int-array
		final int elementPos = (int) (majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
		// The number of value-bits left after the first int
		final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

		if (endBits <= 0) { // Single block
			blocks[elementPos] = blocks[elementPos] & ~(maskRight << -endBits)
					| (value << -endBits);
			return;
		}
		// Two blocks
		blocks[elementPos] = blocks[elementPos] & ~(maskRight >>> endBits)
				| (value >>> endBits);
		blocks[elementPos + 1] = blocks[elementPos + 1] & (~0 >>> endBits)
				| (value << (BLOCK_SIZE - endBits));
	}
	public int[] getBlocks() {
		return blocks;
	}

	public int getBitsPerValue() {
		return bitsPerValue;
	}

}


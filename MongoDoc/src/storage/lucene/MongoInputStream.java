package storage.lucene;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class MongoInputStream extends IndexInput {
	static final int BUFFER_SIZE = MongoOutputStream.BUFFER_SIZE;

	private MongoFile file;
	private long length;

	private byte[] currentBuffer;
	private int currentBufferIndex;

	private int bufferPosition;
	private long bufferStart;
	private int bufferLength;
	
	MongoInputStream(MongoFile file) throws IOException {
		this.file = file;
	    length = file.getLength();
	    if (length/BUFFER_SIZE >= Integer.MAX_VALUE) {
	      throw new IOException("Too large MongoFile! "+length); 
	    }

	    // make sure that we switch to the
	    // first needed buffer lazily
	    currentBufferIndex = -1;
	    currentBuffer = null;

	}

	private final void switchCurrentBuffer(boolean enforceEOF) throws IOException {
		bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
		if (currentBufferIndex >= file.numBuffers()) {
			// end of file reached, no more buffers left
			if (enforceEOF)
				throw new IOException("Read past EOF");
			else {
				// Force EOF if a read takes place at this position
				currentBufferIndex--;
				bufferPosition = BUFFER_SIZE;
			}
		} else {
			currentBuffer = file.getBuffer(currentBufferIndex);
			bufferPosition = 0;
			long buflen = length - bufferStart;
			bufferLength = buflen > BUFFER_SIZE ? BUFFER_SIZE : (int) buflen;
		}
	}
	
	@Override
	public void close() throws IOException {
		// Nothing to do
	}

	@Override
	public long getFilePointer() {
	    return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
	}

	@Override
	public void seek(long pos) throws IOException {
	    if (currentBuffer==null || pos < bufferStart || pos >= bufferStart + BUFFER_SIZE) {
	        currentBufferIndex = (int) (pos / BUFFER_SIZE);
	        switchCurrentBuffer(false);
	      }
	      bufferPosition = (int) (pos % BUFFER_SIZE);

	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public byte readByte() throws IOException {
		if (bufferPosition >= bufferLength) {
			currentBufferIndex++;
			switchCurrentBuffer(true);
		}
		return currentBuffer[bufferPosition++];
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		while (len > 0) {
			if (bufferPosition >= bufferLength) {
				currentBufferIndex++;
				switchCurrentBuffer(true);
			}

			int remainInBuffer = bufferLength - bufferPosition;
			int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
			System.arraycopy(currentBuffer, bufferPosition, b, offset, bytesToCopy);
			offset += bytesToCopy;
			len -= bytesToCopy;
			bufferPosition += bytesToCopy;
		}

	}
	
	@Override
	public void copyBytes(IndexOutput out, long numBytes) throws IOException {
		assert numBytes >= 0: "numBytes=" + numBytes;

		long left = numBytes;
		while (left > 0) {
			if (bufferPosition == bufferLength) {
				++currentBufferIndex;
				switchCurrentBuffer(true);
			}

			final int bytesInBuffer = bufferLength - bufferPosition;
			final int toCopy = (int) (bytesInBuffer < left ? bytesInBuffer : left);
			out.writeBytes(currentBuffer, bufferPosition, toCopy);
			bufferPosition += toCopy;
			left -= toCopy;
		}

		assert left == 0: "Insufficient bytes to copy: numBytes=" + numBytes + " copied=" + (numBytes - left);
	}
	

}

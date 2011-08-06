package storage.lucene;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

public class MongoOutputStream extends IndexOutput {
	static final int BUFFER_SIZE = 1024 * 32;
	
	private MongoFile file;
	
	private byte[] currentBuffer;
	private int currentBufferIndex;

	private int bufferPosition;
	private long bufferStart;
	private int bufferLength;
	
	public MongoOutputStream(MongoFile file) {
		this.file = file;
		currentBufferIndex = -1;
		currentBuffer = null;
	}
	
	private void setFileLength() {
		if (currentBuffer != null) {
			file.writeBuffer(currentBufferIndex, currentBuffer);
		}
		long pointer = bufferStart + bufferPosition;
		if (pointer > file.getLength()) {
			file.setLength(pointer);
		}
	}

	private final void switchCurrentBuffer() throws IOException {
		if (currentBufferIndex == file.numBuffers()) {
			currentBuffer = file.addBuffer(BUFFER_SIZE);
		} else {
			currentBuffer = file.getBuffer(currentBufferIndex);
		}
		bufferPosition = 0;
		bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
		bufferLength = currentBuffer.length;
	}

	
	@Override
	public void flush() throws IOException {
		file.setLastModified(System.currentTimeMillis());
/*		if (currentBuffer != null) {
			file.writeBuffer(currentBufferIndex, currentBuffer);
		}
*/		setFileLength();
	}

	@Override
	public void close() throws IOException {
		flush();

	}

	@Override
	public long getFilePointer() {
		return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
	}

	@Override
	public void seek(long pos) throws IOException {
	    setFileLength();
	    if (pos < bufferStart || pos >= bufferStart + bufferLength) {
	      currentBufferIndex = (int) (pos / BUFFER_SIZE);
	      switchCurrentBuffer();
	    }

	    bufferPosition = (int) (pos % BUFFER_SIZE);

	}

	@Override
	public long length() throws IOException {
		return file.getLength();
	}

	@Override
	public void writeByte(byte b) throws IOException {
		if (bufferPosition == bufferLength) {
			setFileLength();
			currentBufferIndex++;
			switchCurrentBuffer();
		}
		currentBuffer[bufferPosition++] = b;
	}

	@Override
	public void writeBytes(byte[] b, int offset, int len) throws IOException {
		assert b != null;
		while (len > 0) {
			if (bufferPosition ==  bufferLength) {
				setFileLength();
				currentBufferIndex++;
				switchCurrentBuffer();
			}

			int remainInBuffer = currentBuffer.length - bufferPosition;
			int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
			System.arraycopy(b, offset, currentBuffer, bufferPosition, bytesToCopy);
			offset += bytesToCopy;
			len -= bytesToCopy;
			bufferPosition += bytesToCopy;
		}

	}

	@Override
	public void copyBytes(DataInput input, long numBytes) throws IOException {
		assert numBytes >= 0: "numBytes=" + numBytes;

		while (numBytes > 0) {
			if (bufferPosition == bufferLength) {
				setFileLength();
				currentBufferIndex++;
				switchCurrentBuffer();
			}

			int toCopy = currentBuffer.length - bufferPosition;
			if (numBytes < toCopy) {
				toCopy = (int) numBytes;
			}
			input.readBytes(currentBuffer, bufferPosition, toCopy, false);
			numBytes -= toCopy;
			bufferPosition += toCopy;
		}

	}
	
}

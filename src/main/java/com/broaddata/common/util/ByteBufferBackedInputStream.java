
package com.broaddata.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream 
{
    ByteBuffer buf;

    public ByteBufferBackedInputStream(ByteBuffer buf) {
        this.buf = buf;
        this.buf.position(0);
    }

    @Override
    public int read() throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }
    
    @Override
    public int read(byte b[]) throws IOException 
    {
        if (!buf.hasRemaining()) {
            return -1;
        }

        int len = Math.min(b.length, buf.remaining());       
        
        if ( b.length < buf.remaining() )
            buf.get(b);
        else
            buf.get(b, 0, buf.remaining());

        return len;
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException 
    {
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len; 
    }
 
    @Override
    public int available() throws IOException {
        return buf.remaining();
    }
}

package upem.jarret.http;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class HTTPReader {

    private final SocketChannel sc;
    private final ByteBuffer buff;

    public HTTPReader(SocketChannel sc, ByteBuffer buff) {
        this.sc = sc;
        this.buff = buff;
    }

    /**
     * @return The ASCII string terminated by CRLF
     * <p>
     * The method assume that buff is in write mode and leave it in write-mode
     * The method never reads from the socket as long as the buffer is not empty
     * @throws IOException HTTPException if the connection is closed before a line could be read
     */
    public String readLineCRLF() throws IOException {
        buff.flip();
        
        boolean lastChar = false;
        StringBuilder line = new StringBuilder();
        do {
            if(!buff.hasRemaining()) {
                buff.clear();
                if(sc.read(buff) == -1) {
                    throw new HTTPException();
                }
                buff.flip();
            }
            char b = (char)buff.get();
            if(b == '\n' && lastChar) {
                buff.compact();
                return line.toString();
            }
            if(b == '\r') {
                lastChar = true;
            }
            else {
                if(lastChar) {
                    line.append("\r");
                }
                lastChar = false;
                line.append(b);
            }
        } while(true);
    }

    /**
     * @return The HTTPHeader object corresponding to the header read
     * @throws IOException HTTPException if the connection is closed before a header could be read
     *                     if the header is ill-formed
     */
    public HTTPHeader readHeader() throws IOException {
        String firstLine = readLineCRLF();
        HashMap<String, String> lines = new HashMap<>();
        String line;
        while(!(line = readLineCRLF()).equals("")) {
            String[] token = line.split(": ");
            String key = token[0];
            String value = token[1];
            lines.merge(key, value, (v1,v2) -> v1+"; "+v2);
        }
        return HTTPHeader.create(firstLine, lines);
    }

    /**
     * @param size
     * @return a ByteBuffer in write-mode containing size bytes read on the socket
     * @throws IOException HTTPException is the connection is closed before all bytes could be read
     */
    public ByteBuffer readBytes(int size) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(size);
        buff.flip();
        bb.put(buff);
        readFully(bb, sc);
        return bb;
    }
    
    static boolean readFully(ByteBuffer bb, SocketChannel sc) throws IOException {
        while(sc.read(bb) != -1) {
            if(!bb.hasRemaining()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * @return a ByteBuffer in write-mode containing a content read in chunks mode
     * @throws IOException HTTPException if the connection is closed before the end of the chunks
     *                     if chunks are ill-formed
     */

    public ByteBuffer readChunks() throws IOException {
        int totalSize = 0;
        int size;
        List<ByteBuffer> buffers = new ArrayList<>();
        while((size = Integer.parseInt(readLineCRLF(), 16)) != 0) {
            totalSize += size;
            ByteBuffer bb = readBytes(size);
            bb.flip();
            buffers.add(bb);
            readLineCRLF();
        }
        ByteBuffer total = ByteBuffer.allocate(totalSize);
        for(ByteBuffer bb: buffers) {
            total.put(bb);
        }
        return total;
    }
}

package com.simplekafka.broker;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Partition {

    private static final long MAX_SEGMENT_SIZE = 1024 * 1024;
    private static final int INDEX_ENTRY_SIZE = 16; // 8 bytes do offset + 8 bytes da posição

    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir; // pasta da partição
    private final AtomicLong nextOffset; // offset que a proxima mensagem vai receber
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogFileChannel;
    private final List<SegmentInfo> segments;
    private RandomAccessFile activeIndexFile;
    private FileChannel activeIndexFileChannel;

    public Partition(int id, int leader, List<Integer> followers, String baseDir) throws IOException {

        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;

        nextOffset = new AtomicLong(0);
        lock = new ReentrantReadWriteLock();
        segments = new ArrayList<>();
        initialize();
    }

    public int getId() {
        return id;
    }

    public int getLeader() {
        return leader;
    }

    public List<Integer> getFollowers() {
        return followers;
    }

    public String getBaseDir() {
        return baseDir;
    }

    private void initialize() throws IOException {

        File dir = new File(baseDir);
        dir.mkdirs();
        File [] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles != null) {
            for (File logFile : logFiles) {
                String name = logFile.getName();
                String base = name.substring(0, name.length() - 4);
                long baseOffset = Long.parseLong(base);
                File indexFile = new File(baseDir, base + ".index");
                segments.add(new SegmentInfo(baseOffset, logFile, indexFile));
            }
        }
        segments.sort(Comparator.comparingLong(SegmentInfo::getBaseOffset));

        if (segments.isEmpty()) {
            createNewSegment(0);
            return;
        }
        SegmentInfo last = segments.get(segments.size() - 1);
        long count = last.getIndexFile().length() / INDEX_ENTRY_SIZE;
        nextOffset.set(last.getBaseOffset() + count);
        openSegmentForAppend(last);
    }

    private void createNewSegment(long baseOffset) throws  IOException {

        String name = String.format("%020d", baseOffset);

        File logFile = new File(baseDir, name + ".log");
        File indexFile = new  File(baseDir, name + ".index");
        logFile.createNewFile();
        indexFile.createNewFile();

        SegmentInfo segment = new SegmentInfo(baseOffset, logFile, indexFile);
        segments.add(segment);
        openSegmentForAppend(segment);

    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {

        if (activeLogFileChannel != null) {
            activeLogFileChannel.close();
        }
        if (activeIndexFileChannel != null) {
            activeIndexFileChannel.close();
        }

        activeLogFile = new RandomAccessFile(segment.getLogFile(), "rw");
        activeLogFileChannel = activeLogFile.getChannel();
        activeLogFileChannel.position(activeLogFileChannel.size());

        activeIndexFile = new RandomAccessFile(segment.getIndexFile(), "rw");
        activeIndexFileChannel = activeIndexFile.getChannel();
        activeIndexFileChannel.position(activeIndexFileChannel.size());
    }

    public void close() throws  IOException {

        if (activeLogFileChannel != null) {
            activeLogFileChannel.close();
        }
        if (activeIndexFileChannel != null) {
            activeIndexFileChannel.close();
        }
    }

    private static class SegmentInfo {

        private final long baseOffset;
        private final File logFile;
        private final File indexFile;

        public SegmentInfo(long baseOffset, File logFile, File indexFile) {

            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.indexFile = indexFile;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public File getLogFile() {
            return logFile;
        }

        public File getIndexFile() {
            return indexFile;
        }
    }

    public long append(byte[] message) throws  IOException {

        lock.writeLock().lock();
        try {
            if (activeLogFileChannel.size() > 0 && activeLogFileChannel.size() + 4 + message.length > MAX_SEGMENT_SIZE) {
                createNewSegment(nextOffset.get());
            }
            long position = activeLogFileChannel.position();
            ByteBuffer buf = ByteBuffer.allocate( 4 + message.length);
            buf.putInt(message.length);
            buf.put(message);
            buf.flip();
            while (buf.hasRemaining()) {
                activeLogFileChannel.write(buf);
            }
            activeLogFileChannel.force(true);
            long offset = nextOffset.getAndIncrement();
            updateIndex(offset, position);
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateIndex(long offset, long position) throws IOException {

        ByteBuffer buf = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        buf.putLong(offset);
        buf.putLong(position);
        buf.flip();
        while (buf.hasRemaining()) {
            activeIndexFileChannel.write(buf);
        }
        activeIndexFileChannel.force(true);
    }

    public List<byte[]> readMessages(long offset, int maxBytes) throws  IOException {

        lock.readLock().lock();
        try {
            List<byte[]> result = new ArrayList<>();
            SegmentInfo segment = findSegmentForOffset(offset);
            if (segment == null) {
                return result;
            }
            long position = findPositionForOffset(segment, offset);
            if (position < 0) {
                return result;
            }

            int bytesRead = 0;
            int segIndex = segments.indexOf(segment);

            while (segIndex < segments.size()) {
                SegmentInfo current = segments.get(segIndex);
                try (RandomAccessFile logFile = new RandomAccessFile(current.getLogFile(), "r");
                    FileChannel ch = logFile.getChannel()) {
                    while (position < ch.size()) {
                        ByteBuffer buf = ByteBuffer.allocate( 4);
                        ch.read(buf, position);
                        buf.flip();
                        int size = buf.getInt();

                        if (bytesRead + size > maxBytes && !result.isEmpty()) {
                            return result;
                        }
                        ByteBuffer msgBuf = ByteBuffer.allocate(size);
                        ch.read(msgBuf, position + 4);
                        result.add(msgBuf.array());

                        bytesRead += size;
                        position += 4 + size;
                    }
                }
                segIndex++;
                position = 0;
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    private SegmentInfo findSegmentForOffset(long offset) throws IOException {

        if (offset < 0 || offset >= nextOffset.get()) {
            return null;
        }
        for (int i = segments.size() - 1; i >= 0; i--) {
            if (segments.get(i).getBaseOffset() <= offset) {
                return segments.get(i);
            }
        }
        return null;
    }

    private long findPositionForOffset(SegmentInfo segment, long offset) throws IOException {

        long relative = offset - segment.getBaseOffset();
        long indexPosition = relative * INDEX_ENTRY_SIZE;
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexFile(), "r");
            FileChannel ch = indexFile.getChannel()) {
            if (indexPosition + INDEX_ENTRY_SIZE > ch.size()) {
                return - 1;
            }
            ByteBuffer buf = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            ch.read(buf, indexPosition);
            buf.flip();
            buf.getLong();
            return buf.getLong();
        }
    }
}

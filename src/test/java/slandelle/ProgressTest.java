package slandelle;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProgressTest {

    private static final byte[] PATTERN_BYTES = "RatherLargeFileRatherLargeFileRatherLargeFileRatherLargeFile".getBytes(Charset.forName("UTF-16"));
    private static final long STREAMING_CONTENT_LENGTH_MAGIC_VALUE = -1L;

    private File tmpFile;
    protected Server server;
    protected int port;

    public static class EchoHandler extends AbstractHandler {

        public void handle(String pathInContext, Request request, HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException, ServletException {

            // respond with the same body as in the request
            byte[] bytes = new byte[20];
            if (bytes.length > 0) {
                int read = 0;
                while (read > -1) {
                    read = httpRequest.getInputStream().read(bytes);
                    if (read > 0) {
                        httpResponse.getOutputStream().write(bytes, 0, read);
                    }
                }
            }

            httpResponse.setStatus(200);
            httpResponse.getOutputStream().flush();
            httpResponse.getOutputStream().close();
        }
    }

    protected int findFreePort() throws IOException {
        ServerSocket socket = null;

        try {
            socket = new ServerSocket(0);

            return socket.getLocalPort();
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    protected String getTargetUrl() {
        return String.format("http://127.0.0.1:%d/foo/test", port);
    }

    public AbstractHandler configureHandler() throws Exception {
        return new EchoHandler();
    }

    @Before
    public void setUp() throws Exception {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "ahc-tests-" + UUID.randomUUID().toString().substring(0, 8));
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();

        int repeats = (1024 * 1000 / PATTERN_BYTES.length) + 1;
        tmpFile = createTempFile(tmpDir, repeats);

        server = new Server();
        port = findFreePort();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);
        server.setHandler(configureHandler());
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    private static File createTempFile(File dir, int repeat) throws IOException {
        File tmpFile = File.createTempFile("tmpfile-", ".data", dir);
        tmpFile.deleteOnExit();
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(tmpFile);
            for (int i = 0; i < repeat; i++) {
                out.write(PATTERN_BYTES);
            }
        } finally {
            if (out != null) {
                out.close();
            }
        }

        return tmpFile;
    }

    // ------------------------------------------------------------------

    private void send(Object body, long contentLength, long expectedContentLength) throws Exception {
        EventLoopGroup eventLoop = new NioEventLoopGroup();
        Bootstrap plainBootstrap = new Bootstrap().channel(NioSocketChannel.class).group(eventLoop);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicLong lastProgress = new AtomicLong();
        final AtomicLong lastTotal = new AtomicLong();

        try {
            plainBootstrap.handler(new ChannelInitializer<Channel>() {

                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline()//
                            .addLast("http", new HttpClientCodec())//
                            .addLast("chunker", new ChunkedWriteHandler());
                }
            });

            Channel channel = plainBootstrap.connect(new InetSocketAddress("127.0.0.1", port)).sync().channel();

            DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, getTargetUrl());
            request.headers()//
                    .add(HttpHeaders.Names.ACCEPT, "*/*") /**/
                    .add(HttpHeaders.Names.HOST, "*127.0.0.1:" + port) /**/
                    .add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);

            if (contentLength != STREAMING_CONTENT_LENGTH_MAGIC_VALUE) {
                request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, contentLength);
            } else {
                request.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
            }

            channel.write(request);
            ChannelFuture cf = channel.write(body, channel.newProgressivePromise())//
                    .addListener(new ChannelProgressiveFutureListener() {

                        public void operationComplete(ChannelProgressiveFuture cf) {
                            latch.countDown();
                        }

                        public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) throws Exception {
                            System.out.println("progress=" + progress + ", total=" + total);
                            lastProgress.set(progress);
                            lastTotal.set(total);
                        }
                    });
            channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

            cf.sync();
            latch.await();

            // According to GenericProgressiveFutureListener.operationProgress, total is "the number that signifies the end of the operation when {@code progress} reaches at it",
            // meaning that last progress value should be equal to total
            // Assert.assertEquals("wrong last progress", expectedContentLength, lastProgress.get());
            // Assert.assertEquals("wrong last total", expectedContentLength, lastTotal.get());

        } finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    public void testFileUpload() throws Exception {

        RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");
        try {
            send(new DefaultFileRegion(raf.getChannel(), 0, tmpFile.length()), tmpFile.length(), tmpFile.length());
        } finally {
            raf.close();
        }
    }

    @Test
    public void testStreaming() throws Exception {

        final InputStream is = new FileInputStream(tmpFile);
        try {
            send(new ChunkedStream(is), STREAMING_CONTENT_LENGTH_MAGIC_VALUE, tmpFile.length());
        } finally {
            is.close();
        }
    }
}

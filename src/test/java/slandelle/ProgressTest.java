package slandelle;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProgressTest {

    private static final File TMP = new File(System.getProperty("java.io.tmpdir"), "ahc-tests-" + UUID.randomUUID().toString().substring(0, 8));
    private static final byte[] PATTERN_BYTES = "RatherLargeFileRatherLargeFileRatherLargeFileRatherLargeFile".getBytes(Charset.forName("UTF-16"));

    static {
        TMP.mkdirs();
        TMP.deleteOnExit();
    }

    protected Server server;
    protected int port1;

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
        return String.format("http://127.0.0.1:%d/foo/test", port1);
    }

    public AbstractHandler configureHandler() throws Exception {
        return new EchoHandler();
    }

    @Before
    public void setUp() throws Exception {
        server = new Server();

        port1 = findFreePort();

        Connector listener = new SelectChannelConnector();

        listener.setHost("127.0.0.1");
        listener.setPort(port1);

        server.addConnector(listener);

        server.setHandler(configureHandler());
        server.start();
    }

    public void tearDown() throws Exception {
        server.stop();
    }

    // ------------------------------------------------------------------

    @Test
    public void testUploadProgress() throws Exception {
        EventLoopGroup eventLoop = new NioEventLoopGroup();
        Bootstrap plainBootstrap = new Bootstrap().channel(NioSocketChannel.class).group(eventLoop);

        final CountDownLatch latch = new CountDownLatch(1);

        long repeats = (1024 * 100 * 10 / PATTERN_BYTES.length) + 1;
        File file = createTempFile(PATTERN_BYTES, (int) repeats);
        final RandomAccessFile raf = new RandomAccessFile(file, "r");
        final long expectedFileSize = PATTERN_BYTES.length * repeats;
        Assert.assertEquals("Invalid file length", expectedFileSize, file.length());

        final AtomicLong lastProgress = new AtomicLong();

        try {
            plainBootstrap.handler(new ChannelInitializer<Channel>() {

                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline() /**/
                    .addLast("httpHandler", new HttpClientCodec());
                }
            });

            ChannelFuture f = plainBootstrap.connect(new InetSocketAddress("127.0.0.1", port1));

            f.addListener(new ChannelFutureListener() {

                public void operationComplete(ChannelFuture future) throws Exception {

                    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, getTargetUrl());
                    request.headers() /**/
                    .add(HttpHeaders.Names.ACCEPT, "*/*") /**/
                    .set(HttpHeaders.Names.CONTENT_LENGTH, expectedFileSize) /**/
                    .add(HttpHeaders.Names.HOST, "*127.0.0.1:" + port1) /**/
                    .add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);

                    future.channel().write(request);
                    future.channel().write(new DefaultFileRegion(raf.getChannel(), 0, expectedFileSize), future.channel().newProgressivePromise())/**/
                    .addListener(new ChannelProgressiveFutureListener() {

                        public void operationComplete(ChannelProgressiveFuture cf) {
                            latch.countDown();
                        }

                        public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) throws Exception {
                            System.out.println("progress=" + progress + ", total=" + total);
                            lastProgress.set(progress);
                        }
                    });
                    future.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                }
            });
            latch.await();

            // According to GenericProgressiveFutureListener.operationProgress, total is "the number that signifies the end of the operation when {@code progress} reaches at it",
            // meaning that last progress value should be equal to total
            Assert.assertEquals("operationProgressed wasn't notified of all the bytes", expectedFileSize, lastProgress.get());

        } finally {
            eventLoop.shutdownGracefully();
            raf.close();
        }
    }

    private static File createTempFile(byte[] pattern, int repeat) throws IOException {
        File tmpFile = File.createTempFile("tmpfile-", ".data", TMP);
        tmpFile.deleteOnExit();
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(tmpFile);
            for (int i = 0; i < repeat; i++) {
                out.write(pattern);
            }
        } finally {
            if (out != null) {
                out.close();
            }
        }

        return tmpFile;
    }
}

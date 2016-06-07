 /*
 * Copyright 2009, Mahmood Ali.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following disclaimer
 *     in the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Mahmood Ali. nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.notnoop.apns.internal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import com.notnoop.apns.ApnsDelegate;
import com.notnoop.apns.StartSendingApnsDelegate;
import com.notnoop.apns.ApnsNotification;
import com.notnoop.apns.DeliveryError;
import com.notnoop.apns.EnhancedApnsNotification;
import com.notnoop.apns.ReconnectPolicy;
import com.notnoop.exceptions.ApnsDeliveryErrorException;
import com.notnoop.exceptions.NetworkIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApnsConnectionImpl implements ApnsConnection {

    private static final Logger logger = LoggerFactory.getLogger(ApnsConnectionImpl.class);

    private final SocketFactory factory;
    private final String host;
    private final int port;
    private final int readTimeout;
    private final int connectTimeout;
    private final Proxy proxy;
    private final String proxyUsername;
    private final String proxyPassword;
    private final ReconnectPolicy reconnectPolicy;
    private final ApnsDelegate delegate;
    private int cacheLength;
    private final boolean errorDetection;
    private final ThreadFactory threadFactory;
    private final boolean autoAdjustCacheLength;
    private ApnsSocket apnsSocket;
    private final AtomicInteger threadId = new AtomicInteger(0);

    public ApnsConnectionImpl(SocketFactory factory, String host, int port) {
        this(factory, host, port, new ReconnectPolicies.Never(), ApnsDelegate.EMPTY);
    }

    private ApnsConnectionImpl(SocketFactory factory, String host, int port, ReconnectPolicy reconnectPolicy, ApnsDelegate delegate) {
        this(factory, host, port, null, null, null, reconnectPolicy, delegate);
    }

    private ApnsConnectionImpl(SocketFactory factory, String host, int port, Proxy proxy, String proxyUsername, String proxyPassword,
                               ReconnectPolicy reconnectPolicy, ApnsDelegate delegate) {
        this(factory, host, port, proxy, proxyUsername, proxyPassword, reconnectPolicy, delegate, false, null,
                ApnsConnection.DEFAULT_CACHE_LENGTH, true, 0, 0);
    }

    public ApnsConnectionImpl(SocketFactory factory, String host, int port, Proxy proxy, String proxyUsername, String proxyPassword,
                              ReconnectPolicy reconnectPolicy, ApnsDelegate delegate, boolean errorDetection, ThreadFactory tf, int cacheLength,
                              boolean autoAdjustCacheLength, int readTimeout, int connectTimeout) {
    	
        this.factory = factory;
        this.host = host;
        this.port = port;
        this.reconnectPolicy = reconnectPolicy;
        this.delegate = delegate == null ? ApnsDelegate.EMPTY : delegate;
        this.proxy = proxy;
        this.errorDetection = errorDetection;
        this.threadFactory = tf == null ? defaultThreadFactory() : tf;
        this.cacheLength = cacheLength;
        this.autoAdjustCacheLength = autoAdjustCacheLength;
        this.readTimeout = readTimeout;
        this.connectTimeout = connectTimeout;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
    }

    private ThreadFactory defaultThreadFactory() {
        return new ThreadFactory() {
            @Override
            public Thread newThread( final Runnable r )
            {
                Thread result = new Thread(r) {
                	@Override
                	public void start() {
                		try {
                			synchronized(r) {
                				super.start();
                				r.wait();
                			}
						} catch (InterruptedException e) {
							logger.warn("thread start interrupted for thread " + this);
						}
                	}
                };
                result.setName("MonitoringThread-"+threadId.incrementAndGet());
                result.setDaemon(true);
                result.setPriority(Math.min(result.getPriority()+1, Thread.MAX_PRIORITY));
                return result;
            }
        };
    }

    public synchronized void close() {
    	logger.warn("closing apns connection");
        close(apnsSocket);
        shutdownExecutors();
    }
    
    public synchronized void close(ApnsSocket apnsSocketToClose) {
    	if (apnsSocketToClose == null) {
    		return;
    	}
    	
    	Socket socketToClose = apnsSocketToClose.getSocket();
        Utilities.close(socketToClose);
        // also, clear object reference to apns socket as socket.isClose() may be unreliable
        if (apnsSocket == apnsSocketToClose) {
        	apnsSocket = null;
        }
    }
    
    private synchronized void shutdownExecutors() {
    	logger.warn("shutting down executors");
    	serialExecutor.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!serialExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
				serialExecutor.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!serialExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
					logger.warn("serialExecutor did not terminate");
				}
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			serialExecutor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
    }

    ExecutorService serialExecutor = Executors.newSingleThreadExecutor();
    private void monitorSocket(final ApnsSocket currentApnsSocket) {
    	final Socket currentSocket = currentApnsSocket.getSocket();
    	final ConcurrentLinkedQueue<ApnsNotification> cachedNotifications = currentApnsSocket.getCachedNotifications();
    	
        logger.debug("Launching Monitoring Thread for socket {}", currentSocket);

        Thread t = threadFactory.newThread(new Runnable() {
            final static int EXPECTED_SIZE = 6;

            @Override
            public void run() {
                logger.debug("Started monitoring thread");
                final ConcurrentLinkedQueue<ApnsNotification> notificationsBuffer = new ConcurrentLinkedQueue<ApnsNotification>();
                try {
                    InputStream in;
                    try {
                        in = currentSocket.getInputStream();
                    } catch (IOException ioe) {
                        in = null;
                    }

                    byte[] bytes = new byte[EXPECTED_SIZE];
                    synchronized(this) {
                    	// notify wait in start() method that ensures this thread is off and running
                    	this.notifyAll();
                    }
                    while (in != null && readPacket(in, bytes)) {
                        // Quickly close socket, so we won't ever try to send push notifications
                        // using the defective socket.
                    	logger.debug("error found, closing connection");
                        close(currentApnsSocket);
                        logger.debug("closed connection");
                        
                    	synchronized (cachedNotifications) {
                        	try {
		                        logger.debug("Error-response packet {}", Utilities.encodeHex(bytes));
	
		                        int command = bytes[0] & 0xFF;
		                        if (command != 8) {
		                            throw new IOException("Unexpected command byte " + command);
		                        }
		                        int statusCode = bytes[1] & 0xFF;
		                        DeliveryError e = DeliveryError.ofCode(statusCode);
	
		                        int id = Utilities.parseBytes(bytes[2], bytes[3], bytes[4], bytes[5]);
	
		                        logger.debug("Closed connection cause={}; id={}", e, id);
		                        delegate.connectionClosed(e, id);
	
		                        Queue<ApnsNotification> tempCache = new LinkedList<ApnsNotification>();
		                        ApnsNotification failedNotification = null;
	
		                        while (!cachedNotifications.isEmpty()) {
		                        	ApnsNotification notification = cachedNotifications.poll();
		                            logger.debug("Candidate for removal, message id {}", notification.getIdentifier());
	
		                            if (notification.getIdentifier() == id) {
		                                logger.debug("Bad message found {}", notification.getIdentifier());
		                                failedNotification = notification;
		                                break;
		                            }
		                            tempCache.add(notification);
		                        }
	
		                        if (failedNotification != null) { // notification found
		                            logger.debug("delegate.messageSendFailed, message id {}", failedNotification.getIdentifier());
		                            delegate.messageSendFailed(failedNotification, new ApnsDeliveryErrorException(e));
		                            
		                            // in some cases we can retry the message the failure occurred on
		                            if ( e.shouldRetryFailedDelivery() )
		                            	notificationsBuffer.add(failedNotification);
		                        } else {
		                            cachedNotifications.addAll(tempCache);
		                            int resendSize = tempCache.size();
		                            logger.warn("Received error for message that wasn't in the cache...");
		                            if (autoAdjustCacheLength) {
		                                cacheLength = cacheLength + (resendSize / 2);
		                                delegate.cacheLengthExceeded(cacheLength);
		                            }
		                            logger.debug("delegate.messageSendFailed, unknown id");
		                            delegate.messageSendFailed(null, new ApnsDeliveryErrorException(e));
		                        }
	
		                        while (!cachedNotifications.isEmpty()) {
		                            final ApnsNotification resendNotification = cachedNotifications.poll();
		                            logger.debug("Queuing for resend {}", resendNotification.getIdentifier());
		                            notificationsBuffer.add(resendNotification);
		                        }
		                        logger.debug("resending {} notifications", notificationsBuffer.size());
		                        delegate.notificationsResent(notificationsBuffer.size());
    	                    }
    	                    finally {
    	                    	serialExecutor.execute( new Runnable() {
    	                    		public void run() {
    	            	                drainBuffer(notificationsBuffer);                			
    	                    		}
    	                    	});
    	                    }
                        	
	                    	logger.debug("Monitoring input stream closed by EOF");

                    	} // end synchronization on cachedNotifications
                    }
                } catch (IOException e) {
                    // An exception when reading the error code is non-critical, it will cause another retry
                    // sending the message. Other than providing a more stable network connection to the APNS
                    // server we can't do much about it - so let's not spam the application's error log.
                    logger.info("Exception while waiting for error code", e);
                    close(currentApnsSocket);
                    delegate.connectionClosed(DeliveryError.UNKNOWN, -1);
                }
            }

            /**
             * Read a packet like in.readFully(bytes) does - but do not throw an exception and return false if nothing
             * could be read at all.
             * @param in the input stream
             * @param bytes the array to be filled with data
             * @return true if a packet as been read, false if the stream was at EOF right at the beginning.
             * @throws IOException When a problem occurs, especially EOFException when there's an EOF in the middle of the packet.
             */
            private boolean readPacket(final InputStream in, final byte[] bytes) throws IOException {
                final int len = bytes.length;
                int n = 0;
                while (n < len) {
                    try {
                        int count = in.read(bytes, n, len - n);
                        if (count < 0) {
                            throw new EOFException("EOF after reading "+n+" bytes of new packet.");
                        }
                        n += count;
                    } catch (IOException ioe) {
                        if (n == 0)
                            return false;
                        throw new IOException("Error after reading "+n+" bytes of packet", ioe);
                    }
                }
                return true;
            }
        });
        t.start();
    }
    
    private class ApnsSocket {
    	private final Socket socket;
    	private final ConcurrentLinkedQueue<ApnsNotification> cachedNotifications;
    	
    	public Socket getSocket() {
			return socket;
		}

		public ConcurrentLinkedQueue<ApnsNotification> getCachedNotifications() {
			return cachedNotifications;
		}

		public ApnsSocket(Socket socket, ConcurrentLinkedQueue<ApnsNotification> cachedNotifications) {
    		this.socket = socket;
    		this.cachedNotifications = cachedNotifications;
    	}
    	
    	
    }

    private synchronized ApnsSocket getOrCreateSocket(boolean resend) throws NetworkIOException {
    	ApnsSocket currentApnsSocket = apnsSocket;
    	Socket currentSocket = currentApnsSocket != null ? apnsSocket.getSocket() : null;
    	
        if (reconnectPolicy.shouldReconnect()) {
            logger.debug("Reconnecting due to reconnectPolicy dictating it");
            close(currentApnsSocket);
            currentSocket = null;
        }

        if (currentSocket == null || currentSocket.isClosed()) {
            try {
                if (proxy == null) {
                	currentSocket = factory.createSocket(host, port);
                    logger.debug("Connected new socket {}", currentSocket);
                } else if (proxy.type() == Proxy.Type.HTTP) {
                    TlsTunnelBuilder tunnelBuilder = new TlsTunnelBuilder();
                    currentSocket = tunnelBuilder.build((SSLSocketFactory) factory, proxy, proxyUsername, proxyPassword, host, port);
                    logger.debug("Connected new socket through http tunnel {}", currentSocket);
                } else {
                    boolean success = false;
                    Socket proxySocket = null;
                    try {
                        proxySocket = new Socket(proxy);
                        proxySocket.connect(new InetSocketAddress(host, port), connectTimeout);
                        currentSocket = ((SSLSocketFactory) factory).createSocket(proxySocket, host, port, false);
                        success = true;
                    } finally {
                        if (!success) {
                            Utilities.close(proxySocket);
                        }
                    }
                    logger.debug("Connected new socket through socks tunnel {}", currentSocket);
                }

                currentSocket.setSoTimeout(readTimeout);
                currentSocket.setKeepAlive(true);
                ConcurrentLinkedQueue<ApnsNotification> cachedNotifications = new ConcurrentLinkedQueue<ApnsNotification>();
                
                currentApnsSocket = new ApnsSocket(currentSocket, cachedNotifications);
                
                if (errorDetection) {
                    monitorSocket(currentApnsSocket);
                }

                reconnectPolicy.reconnected();
                logger.debug("Made a new connection to APNS");
            } catch (IOException e) {
                logger.error("Couldn't connect to APNS server", e);
                // indicate to clients whether this is a resend or initial send
                throw new NetworkIOException(e, resend);
            }
        }
        
        apnsSocket = currentApnsSocket;
        
        return apnsSocket;
    }

    int DELAY_IN_MS = 1000;
    private static final int RETRIES = 3;

    public synchronized void sendMessage(ApnsNotification m) throws NetworkIOException {
        sendMessage(m, false);
    }

    private synchronized void sendMessage(ApnsNotification m, boolean fromBuffer) throws NetworkIOException {
        logger.debug("sendMessage {} fromBuffer: {}", m, fromBuffer);

        if (delegate instanceof StartSendingApnsDelegate) {
            ((StartSendingApnsDelegate) delegate).startSending(m, fromBuffer);
        }

        int attempts = 0;
        ApnsSocket currentApnsSocket = null;
        Socket currentSocket = null;
        while (true) {
            try {
                attempts++;
            	try {
	                currentApnsSocket = getOrCreateSocket(fromBuffer);
            		currentSocket = currentApnsSocket.getSocket();
	                currentSocket.getOutputStream().write(m.marshall());
	                currentSocket.getOutputStream().flush();
            	} finally {
            		// if the send fails, we want to keep the message in the cache so that retrying is possible
            		cacheNotification(currentApnsSocket.getCachedNotifications(), m);
            	}

                delegate.messageSent(m, fromBuffer);

                //logger.debug("Message \"{}\" sent", m);
                attempts = 0;
                break;
            } catch (IOException e) {
                close(currentApnsSocket);
                if (attempts >= RETRIES) {
                    logger.error("Couldn't send message after " + RETRIES + " retries." + m, e);
                    delegate.messageSendFailed(m, e);
                    Utilities.wrapAndThrowAsRuntimeException(e);
                }
                // The first failure might be due to closed connection (which in turn might be caused by
                // a message containing a bad token), so don't delay for the first retry.
                //
                // Additionally we don't want to spam the log file in this case, only after the second retry
                // which uses the delay.

                if (attempts != 1) {
                    logger.info("Failed to send message " + m + "... trying again after delay", e);
                    Utilities.sleep(DELAY_IN_MS);
                }
            }
        }
    }

    private void drainBuffer(ConcurrentLinkedQueue<ApnsNotification> notificationsBuffer) {
        logger.debug("draining buffer");
        while (!notificationsBuffer.isEmpty()) {
            final ApnsNotification notification = notificationsBuffer.poll();
            try {
                sendMessage(notification, true);
            }
            catch (NetworkIOException ex) {
                // at this point we are retrying the submission of messages but failing to connect to APNS, therefore
                // notify the client of this
                delegate.messageSendFailed(notification, ex);
            }
        }
    }

    private void cacheNotification(ConcurrentLinkedQueue<ApnsNotification> cachedNotifications, ApnsNotification notification) {
    	synchronized( cachedNotifications ) {
	        cachedNotifications.add(notification);
	        while (cachedNotifications.size() > cacheLength) {
	            cachedNotifications.poll();
	            logger.debug("Removing notification from cache " + notification);
	        }
    	}
    }

    public ApnsConnectionImpl copy() {
        return new ApnsConnectionImpl(factory, host, port, proxy, proxyUsername, proxyPassword, reconnectPolicy.copy(), delegate,
                errorDetection, threadFactory, cacheLength, autoAdjustCacheLength, readTimeout, connectTimeout);
    }

    public void testConnection() throws NetworkIOException {
        ApnsConnectionImpl testConnection = null;
        try {
            testConnection =
                    new ApnsConnectionImpl(factory, host, port, proxy, proxyUsername, proxyPassword, reconnectPolicy.copy(), delegate);
            final ApnsNotification notification = new EnhancedApnsNotification(0, 0, new byte[]{0}, new byte[]{0});
            testConnection.sendMessage(notification);
        } finally {
            if (testConnection != null) {
                testConnection.close();
            }
        }
    }

    public void setCacheLength(int cacheLength) {
        this.cacheLength = cacheLength;
    }

    public int getCacheLength() {
        return cacheLength;
    }
}

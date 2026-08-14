/*
 * Copyright (c) 2025 lax1dude. All Rights Reserved.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 */

package net.lax1dude.eaglercraft.backend.server.base.pipeline;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import net.lax1dude.eaglercraft.backend.server.base.message.InjectedMessage;
import net.lax1dude.eaglercraft.backend.server.base.message.InjectedMessageController;

public class EaglerInjectedMessageHandler extends MessageToMessageCodec<ByteBuf, InjectedMessage> {

	private static final int MAX_PENDING_PACKETS = 64;
	private static final int MAX_PENDING_BYTES = 1024 * 1024;

	private InjectedMessageController injectedController;
	private List<ByteBuf> pendingPackets;
	private int pendingBytes;

	public EaglerInjectedMessageHandler() {
	}

	public EaglerInjectedMessageHandler(InjectedMessageController injectedController) {
		this.injectedController = injectedController;
	}

	public void setController(Channel channel, InjectedMessageController injectedController) {
		Runnable task = () -> {
			this.injectedController = injectedController;
			if (pendingPackets != null) {
				try {
					for (ByteBuf packet : pendingPackets) {
						injectedController.readPacket(packet);
					}
				} finally {
					for (ByteBuf packet : pendingPackets) {
						packet.release();
					}
					pendingPackets = null;
					pendingBytes = 0;
				}
			}
		};
		if (channel.eventLoop().inEventLoop()) {
			task.run();
		} else {
			channel.eventLoop().execute(task);
		}
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, InjectedMessage msg, List<Object> output) throws Exception {
		msg.writePacket(output);
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> output) throws Exception {
		if (msg.readableBytes() > 0 && msg.getUnsignedByte(msg.readerIndex()) == 0xEE) {
			if (injectedController != null) {
				injectedController.readPacket(msg);
			} else {
				int len = msg.readableBytes();
				if ((pendingPackets != null && pendingPackets.size() >= MAX_PENDING_PACKETS)
						|| len > MAX_PENDING_BYTES - pendingBytes) {
					ctx.close();
					return;
				}
				if (pendingPackets == null) {
					pendingPackets = new ArrayList<>();
				}
				pendingPackets.add(msg.retain());
				pendingBytes += len;
			}
		} else {
			output.add(msg.retain());
		}
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		if (pendingPackets != null) {
			for (ByteBuf packet : pendingPackets) {
				packet.release();
			}
			pendingPackets = null;
			pendingBytes = 0;
		}
		super.handlerRemoved(ctx);
	}

}
